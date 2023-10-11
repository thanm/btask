// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package btask

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type TaskStat int

const (
	StUnknown TaskStat = 0 + iota

	// Sent from task to taskmon: I am runnable (all in-deps satisfied)
	StRunnable

	// Sent from taskmon to task: you now have the perf lock
	StPerfGranted

	// Sent from task to taskmon: resume run now
	StResume

	// Sent from task to taskmon: here is my task status
	StRunResult
)

var VerbLevel int
var PreserveTemps bool

// IdStat abstracts the result generated from execution of a task: the
// ID of the task and the resulting status. Status is 0 for success,
// positive for failure, and negative for "not run" (due to failure of
// a dependent task).
type IdStat struct {
	Id     int
	Which  TaskStat
	Status int
}
type DepChan chan IdStat

// A task to perform as part of the run. A task is basically a command to
// execute and wait for, plus a set of dependencies expressed as channels.

type Task struct {
	Id           int       // unique task id
	Name         string    // name (ex: "benchprep gollvm")
	Command      []string  // command to execute
	Cmd          *exec.Cmd // command returned from os.Command
	MonChan      DepChan
	NumIn        int
	InDeps       DepChan
	OutDeps      []DepChan
	PerfCritical bool // performance critical command, run in isolation
	ErrOutFile   string
	ShellTF      *os.File
	Duration     int // running time in milliseconds
	ResultStat   int // exit status
}

// Clients create an object of this type to pass to the task
// monitor when creating new tasks.

type TaskSetup struct {
	Name         string // name (ex: "benchprep gollvm")
	Command      string // command to execute
	ErrOutFile   string
	PerfCritical bool // performance critical command, run in isolation
}

// Task monitoring helper. Provides methods for creating and then
// executing tasks.
//
// A note on MonChan: each task is expected to send

type TaskMon struct {
	tasks         []*Task
	taskByName    map[string]*Task
	taskByChan    map[DepChan]*Task
	monChan       chan IdStat
	sigChan       chan os.Signal
	tempfiles     []*os.File
	perfqueue     []*Task
	indeps        map[int]int
	numRunning    int
	perfCritCount int
}

func MakeTaskMon() *TaskMon {
	m := new(TaskMon)
	m.taskByName = make(map[string]*Task)
	m.taskByChan = make(map[DepChan]*Task)
	m.monChan = make(DepChan, 1000)
	m.sigChan = make(chan os.Signal, 1)
	signal.Notify(m.sigChan, os.Interrupt)
	go m.monitorSigChan()
	m.tempfiles = []*os.File{}
	m.perfqueue = []*Task{}
	m.indeps = make(map[int]int)
	return m
}

func xwarn(s string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, s, a...)
	fmt.Fprintf(os.Stderr, "\n")
}

func xerror(s string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, s, a...)
	fmt.Fprintf(os.Stderr, "\n")
	os.Exit(1)
}

func xverb(vlevel int, s string, a ...interface{}) {
	if VerbLevel >= vlevel {
		fmt.Printf(s, a...)
		fmt.Printf("\n")
	}
}

func xpanic(s string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, s, a...)
	fmt.Fprintf(os.Stderr, "\n")
	panic("internal error")
}

func TempContents(tn string) string {
	var sb strings.Builder
	f, err := os.Open(tn)
	if err != nil {
		return "<err opening>"
	}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		line = strings.Replace(line, "\"", "\\\"", -1)
		sb.WriteString(line + "\\n")
	}
	f.Close()
	return sb.String()
}

func (tm *TaskMon) monitorSigChan() {
	s := <-tm.sigChan
	fmt.Fprintf(os.Stderr, "** received signal %v\n", s)
	fmt.Fprintf(os.Stderr, "** killing subtasks...\n")
	for _, t := range tm.tasks {
		if t.Cmd == nil {
			continue
		}
		t.Cmd.Process.Signal(syscall.SIGINT)
	}
	fmt.Fprintf(os.Stderr, "** ... done.\n")
}

func (tm *TaskMon) DumpGraph(fn string) {
	xverb(1, "emitting TaskMon graph to %s", fn)
	tf, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		xpanic("opening graph output file %s: %v", fn, err)
		return
	}
	fmt.Fprintf(tf, "digraph G {\n")

	// nodes
	for _, t := range tm.tasks {
		fmt.Fprintf(tf, "  \"task%d\" ", t.Id)
		shl := ""
		if t.ShellTF != nil {
			shl = TempContents(t.ShellTF.Name())
		}
		shape := "ellipse"
		if t.PerfCritical {
			shape = "parallelogram"
		}
		color := ""
		if t.ResultStat == -1 {
			color = ", color=magenta"
		} else if t.ResultStat != 0 {
			color = ", color=red"
		}
		duration := ""
		if t.Duration != 0 {
			if t.Duration < 1000 {
				duration = fmt.Sprintf("%d ms", t.Duration)
			} else {
				duration = fmt.Sprintf("%2.1f seconds", float64(t.Duration)/float64(1000))
			}
		}
		fmt.Fprintf(tf, " [label=\"Task %d: '%s'\\n%s\\n%s\\n%s\", shape=%s%s]\n",
			t.Id, t.Name, duration, strings.Join(t.Command, " "),
			shl, shape, color)
	}

	// edges
	for _, t := range tm.tasks {
		for _, c := range t.OutDeps {
			dt := tm.taskByChan[c]
			fmt.Fprintf(tf, "  \"task%d\" -> \"task%d\"\n", t.Id, dt.Id)
		}
	}
	fmt.Fprintf(tf, "}\n")
	tf.Close()
}

func (tm *TaskMon) Destroy() {
	for _, tf := range tm.tempfiles {
		tf.Close()
		if PreserveTemps {
			xverb(0, "preserving temp file %s", tf.Name())
		} else {
			xverb(2, "removing temp file %s", tf.Name())
			err := os.Remove(tf.Name())
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: unable to remove temp file %s: %v\n",
					tf.Name(), err)
			}
		}
	}
}

func (tm *TaskMon) mkTempFile() *os.File {
	tf, err := ioutil.TempFile("", "btmp")
	if err != nil {
		xpanic("tempfile open failed")
	}
	tm.tempfiles = append(tm.tempfiles, tf)
	return tf
}

func (tm *TaskMon) PreserveTemp(ptf *os.File) {
	for idx, tf := range tm.tempfiles {
		if tf == ptf {
			// Remove element idx from slice
			tm.tempfiles = append(tm.tempfiles[:idx], tm.tempfiles[idx+1:]...)
			return
		}
	}
	xpanic("internal error: tempfile %s not found on taskmon list", ptf.Name())
}

func (tm *TaskMon) MakeShellTask(p TaskSetup) *Task {
	tf := tm.mkTempFile()
	fmt.Fprintf(tf, "%s\n", p.Command)
	dashx := ""
	if VerbLevel > 1 {
		dashx = "-x "
	}
	shcmd := fmt.Sprintf("/bin/sh %s%s", dashx, tf.Name())
	setup := TaskSetup{Name: p.Name, Command: shcmd, ErrOutFile: p.ErrOutFile}
	t := tm.MakeTask(setup)
	t.ShellTF = tf
	return t
}

func (tm *TaskMon) MakeTask(p TaskSetup) *Task {
	if p.Name == "" {
		xpanic("bad task name in MakeTask")
	}
	if _, ok := tm.taskByName[p.Name]; ok {
		xpanic("task with name %s already exists", p.Name)
		return nil
	}
	t := new(Task)
	tm.taskByName[p.Name] = t
	t.Id = len(tm.tasks) + 1
	t.Name = p.Name
	if p.PerfCritical {
		t.PerfCritical = true
		tm.perfCritCount += 1
	}
	t.Command = strings.Split(p.Command, " ")
	t.ErrOutFile = p.ErrOutFile
	t.InDeps = make(DepChan, 1000)
	t.OutDeps = []DepChan{}
	tm.taskByChan[t.InDeps] = t
	t.MonChan = tm.monChan
	tm.tasks = append(tm.tasks, t)
	return t
}

func (tm *TaskMon) LookupTask(name string) *Task {
	if t, ok := tm.taskByName[name]; ok {
		return t
	}
	xpanic("task lookup failed for: '%s'", name)
	return nil
}

func (tm *TaskMon) checkCyclesVisit(nid int, m map[int]int) error {
	xverb(2, "checkCyclesVisit(%d), mapval=%v", nid, m[nid])
	if m[nid] == 1 {
		t := tm.tasks[nid-1]
		return fmt.Errorf("TaskMon: task graph has cycle on %d %q",
			nid, t.Name)
	}
	m[nid] = 1
	t := tm.tasks[nid-1]
	for _, oc := range t.OutDeps {
		ot := tm.taskByChan[oc]
		if err := tm.checkCyclesVisit(ot.Id, m); err != nil {
			return err
		}
	}
	m[nid] = 2
	return nil
}

func (tm *TaskMon) checkCycles() error {
	m := make(map[int]int)
	for _, t := range tm.tasks {
		if t.NumIn != 0 {
			if err := tm.checkCyclesVisit(t.Id, m); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tm *TaskMon) launchDependent(completed *Task) {
	for _, c := range completed.OutDeps {
		dt := tm.taskByChan[c]
		dtnin := tm.indeps[dt.Id]
		if dtnin < 1 {
			xpanic("internal error: task %d supposed to be dependent on task %d but is shown as having zero indeps\n", dt.Id, completed.Id)
		}
		dtnin -= 1
		tm.indeps[dt.Id] = dtnin
		if dtnin == 0 {
			if dt.PerfCritical {
				tm.perfqueue = append(tm.perfqueue, dt)
				xverb(1, "task %d entering perf queue", dt.Id)
			} else {
				xverb(1, "task %d now runnable", dt.Id)
				tm.numRunning += 1
			}
		}
	}
}

// Run all of the tasks in the perf queue, in order.

func (tm *TaskMon) runPerfQueue() {
	xverb(1, "begin runPerfQueue for %d tasks", len(tm.perfqueue))
	for _, torun := range tm.perfqueue {

		// Send "you have the perf lock" message
		torun.InDeps <- IdStat{Id: -1, Which: StPerfGranted}

		// Wait for the "I am running" message. Since this is the only
		// thing running, we expect to see only this message.
		idst := <-tm.monChan
		if idst.Id != torun.Id || idst.Which != StRunnable {
			xpanic("internal error: following task %d perf run, "+
				"received unexpected message %v", torun.Id, idst)
		}
		xverb(1, "task %d perf run starting", torun.Id)

		// Wait for the "run complete" message. Since this is the only
		// thing running, we expect to see only this message.
		idst = <-tm.monChan
		if idst.Id != torun.Id || idst.Which != StRunResult {
			xpanic("internal error: following task %d perf run, "+
				"received unexpected message %v", torun.Id, idst)
		}
		xverb(1, "task %d perf complete with status %v", idst.Id, idst.Status)
	}
	xverb(1, "runPerfQueue: now invoking launchDependent")

	// Copy the contents of the perf queue (jobs we've just run)
	// then zero it out, reflecting the fact that they are done.
	pqcopy := make([]*Task, len(tm.perfqueue))
	copy(pqcopy, tm.perfqueue)
	tm.perfqueue = tm.perfqueue[:0]

	// Send resume messages to each of the tasks. Note that if there is
	// some new task T that is dependent on any of the things we just run,
	// it may get added to the perf queue in this loop.
	for _, torun := range pqcopy {
		torun.InDeps <- IdStat{Id: -1, Which: StResume}
		tm.launchDependent(torun)
	}
	xverb(1, "runPerfQueue: complete")
}

func (tm *TaskMon) Kickoff() (int, int, error) {

	// Check for cycles
	if err := tm.checkCycles(); err != nil {
		return 0, 0, err
	}

	// Locate the set of tasks that are immediately runnable and add them
	// to the runnable map. If a task is marked as performance critical and
	// has no in-deps, add to the perf queue directly.
	tm.numRunning = 0
	for _, t := range tm.tasks {
		tm.indeps[t.Id] = t.NumIn
		if t.NumIn == 0 {
			if t.PerfCritical {
				tm.perfqueue = append(tm.perfqueue, t)
			} else {
				tm.numRunning += 1
			}
		}
	}

	// Create goroutines to execute each task.
	for _, t := range tm.tasks {
		it := t
		go (func(ft *Task) { ft.Run(tm) })(it)
	}

	xverb(1, "kickoff: %d total tasks, %d runnable %d perfqueue",
		len(tm.tasks), tm.numRunning, len(tm.perfqueue))

	// Read status messages from the channel. We expect to see 2*N messages
	// given N tasks: one "runnable" and one "runstatus", minus any
	// perf critical tasks (
	nMsgsExpected := 2 * (len(tm.tasks) - tm.perfCritCount)
	for i := 0; i < nMsgsExpected; i++ {
		xverb(3, "kickoff iter %d nrunning=%d len(perfqueue)=%d", i,
			tm.numRunning, len(tm.perfqueue))
		idst := <-tm.monChan
		t := tm.tasks[idst.Id-1]
		switch idst.Which {
		case StRunnable:
			xverb(1, "task %d command now running", idst.Id)
		case StRunResult:
			tm.numRunning -= 1
			xverb(1, "task %d complete with status %d", idst.Id, idst.Status)
			tm.launchDependent(t)
		}

		// If we've reached nrunnable == 0, run contents of the perf queue.
		if tm.numRunning == 0 && len(tm.perfqueue) != 0 {
			tm.runPerfQueue()
		}
	}
	xverb(1, "TaskMon.Kickoff complete\n")

	// Return number of failures
	nfail := 0
	notrun := 0
	for _, t := range tm.tasks {
		if t.ResultStat == -1 {
			notrun += 1
		} else if t.ResultStat != 0 {
			nfail += 1
		}
	}
	return nfail, notrun, nil
}

// Mark task "t" as dependent on task "src"

func (t *Task) MarkDep(src *Task) {
	src.OutDeps = append(src.OutDeps, t.InDeps)
	t.NumIn += 1
}

func nowmillis() int {
	now := time.Now()
	nanos := now.UnixNano()
	return int(nanos / 1000000)
}

func (t *Task) RunCommand(tm *TaskMon) int {
	var erroutf *os.File
	tstart := nowmillis()
	if t.ErrOutFile != "" {
		f, err := os.OpenFile(t.ErrOutFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			xpanic("opening output file %s for task %s: %v", t.ErrOutFile, t.Name, err)
		}
		erroutf = f
	}
	xverb(1, "task %d '%s' running: %s", t.Id, t.Name, strings.Join(t.Command, " "))
	cmd := exec.Command(t.Command[0], t.Command[1:]...)
	t.Cmd = cmd
	var err error
	if erroutf != nil {
		cmd.Stdout = erroutf
		cmd.Stderr = erroutf
		err = cmd.Run()
		erroutf.Close()
	} else {
		b, xerr := cmd.CombinedOutput()
		os.Stderr.Write(b)
		err = xerr
	}
	t.Cmd = nil
	returnst := 0
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: task %d '%s' failed with error: %v\n", t.Id, t.Name, err)
		fmt.Fprintf(os.Stderr, "       cmd was: '%s'\n", strings.Join(t.Command, " "))
		if t.ShellTF != nil {
			tm.PreserveTemp(t.ShellTF)
			fmt.Fprintf(os.Stderr, "[preserving file %s for inspection]\n", t.ShellTF.Name())
		}
		returnst = 1
	}
	tend := nowmillis()
	t.Duration = int(tend - tstart)
	t.ResultStat = returnst
	return returnst
}

func (t *Task) Run(tm *TaskMon) {
	// Wait for input deps
	infail := false
	for i := 0; i < t.NumIn; i++ {
		st := <-t.InDeps
		if st.Status != 0 {
			infail = true
		}
	}

	// If perf critical, wait until we get the perf lock
	if t.PerfCritical {
		pst := <-t.InDeps
		if pst.Which != StPerfGranted {
			xpanic("task %d: unexpected msg type %v from monitor", t.Id, pst.Which)
		}
	}

	// Inform the monitor that I am running now
	t.MonChan <- IdStat{Id: t.Id, Which: StRunnable}

	// Run command
	st := 0
	if infail {
		st = -1
		t.ResultStat = -1
	} else {
		st = t.RunCommand(tm)
	}

	// Tell taskmon what happened
	what := IdStat{Id: t.Id, Which: StRunResult, Status: st}
	t.MonChan <- what

	// If perf critical, wait until we get the green light to forward
	// on status to dependent tasks.
	if t.PerfCritical {
		pst := <-t.InDeps
		if pst.Which != StResume {
			xpanic("task %d: unexpected msg type %v from monitor", t.Id, pst.Which)
		}
	}

	// Send along to each out-dep as well
	for _, ch := range t.OutDeps {
		ch <- what
	}
}

func testTaskStuff(td string) error {

	// Create task monitor
	tm := MakeTaskMon()
	if tm == nil {
		return fmt.Errorf("MakeTaskMon returned nil")
	}

	t := func(x string) string {
		return strings.ReplaceAll(x, "/tmp", td)
	}

	// Create tasks
	xp := TaskSetup{
		Name:    "X",
		Command: t("exec /bin/echo foo > /tmp/blarg.txt"),
	}
	x := tm.MakeShellTask(xp)
	ap := TaskSetup{
		Name:       "A",
		Command:    "/bin/echo foo",
		ErrOutFile: t("/tmp/gronk.txt"),
	}
	a := tm.MakeTask(ap)
	bp := TaskSetup{Name: "B", Command: "/bin/sleep 1"}
	b := tm.MakeTask(bp)
	cp := TaskSetup{Name: "C", Command: "/bin/false"}
	c := tm.MakeTask(cp)
	dp := TaskSetup{Name: "D", Command: "/bin/echo bar"}
	d := tm.MakeTask(dp)
	fp := TaskSetup{Name: "F", Command: "/bin/echo grobbity"}
	f := tm.MakeTask(fp)
	yp := TaskSetup{Name: "Y", Command: "echo BAD ; exec /bin/false",
		ErrOutFile: t("/tmp/yout.txt")}
	tm.MakeShellTask(yp)

	ds := t("/tmp/dummy.sh")

	// Helper
	{

		tf, err := os.OpenFile(ds, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return fmt.Errorf("can't open %s for writing: %v", ds, err)
		}
		const perc = "%"
		fmt.Fprintf(tf, "#!/bin/sh\ndate +%ss\necho $* start\n", perc)
		fmt.Fprintf(tf, "sleep 2\necho $* end\ndate +%ss\nexit 0\n", perc)
		tf.Close()
	}

	// Performance critical tasks
	pap := TaskSetup{Name: "PA", Command: t("/tmp/dummy.sh PA"),
		ErrOutFile: t("/tmp/papout.txt"), PerfCritical: true}
	pa := tm.MakeTask(pap)
	pfp := TaskSetup{Name: "PF", Command: t("/tmp/dummy.sh PF"),
		ErrOutFile: t("/tmp/pfpout.txt"), PerfCritical: true}
	pf := tm.MakeTask(pfp)
	pqp := TaskSetup{Name: "PQ", Command: t("/tmp/dummy.sh PQ"),
		ErrOutFile: t("/tmp/pqpout.txt"), PerfCritical: true}
	pq := tm.MakeTask(pqp)
	pwp := TaskSetup{Name: "PW", Command: t("/tmp/dummy.sh PW"),
		ErrOutFile: t("/tmp/pwpout.txt"), PerfCritical: true}
	tm.MakeTask(pwp)

	// Things dependent on the above
	pafterpa := TaskSetup{Name: "afterPA", Command: "/bin/echo after PA"}
	afterpa := tm.MakeTask(pafterpa)
	pafterpf := TaskSetup{Name: "afterPF", Command: "/bin/echo after PF"}
	afterpf := tm.MakeTask(pafterpf)

	// Now add deps
	a.MarkDep(x)
	f.MarkDep(x)
	pf.MarkDep(f)
	b.MarkDep(a)
	pa.MarkDep(a)
	b.MarkDep(c)
	c.MarkDep(a)
	d.MarkDep(c)
	d.MarkDep(b)
	pq.MarkDep(pa)
	pq.MarkDep(pf)
	afterpa.MarkDep(pa)
	afterpf.MarkDep(pf)

	// Dump task graph in DOT format
	tm.DumpGraph(t("/tmp/pre-tasks.dot"))

	// Kickoff
	nfailures, notrun, err := tm.Kickoff()
	if err != nil {
		return err
	}

	// Second dump with times and statuses
	tm.DumpGraph(t("/tmp/post-tasks.dot"))

	// Check failures
	expectedFail := 2
	expectedNotRun := 2
	if nfailures != expectedFail {
		return fmt.Errorf("bad return from Kickoff: "+
			"expected %d failures got %d", expectedFail, nfailures)
	}
	if notrun != expectedNotRun {
		return fmt.Errorf("bad return from Kickoff: "+
			"expected %d notrun got %d", expectedFail, nfailures)
	}
	defer tm.Destroy()
	return nil
}
