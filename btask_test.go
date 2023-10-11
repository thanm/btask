// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package btask

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSuperBasic(t *testing.T) {
	td := t.TempDir()
	tm := MakeTaskMon()
	path := filepath.Join(td, "x.dot")
	sts := TaskSetup{Name: "Q",
		Command: "echo GOOD ; exec /bin/true",
	}
	st := tm.MakeShellTask(sts)
	stl := tm.LookupTask("Q")
	if st != stl {
		t.Errorf("LookupTask(\"Q\") returns %p want %p\n", stl, st)
	}
	tm.DumpGraph(path)
	tm.Destroy()
}

func TestCycleDetection(t *testing.T) {
	tm := MakeTaskMon()
	sts := TaskSetup{Name: "A",
		Command: "echo GOOD ; exec /bin/true",
	}
	st1 := tm.MakeShellTask(sts)
	sts.Name = "B"
	st2 := tm.MakeShellTask(sts)
	st1.MarkDep(st2)
	st2.MarkDep(st1)
	_, _, err := tm.Kickoff()
	if err == nil {
		t.Errorf("expected cycle, didn't detect")
	}
	t.Logf("expected cycle error is %v\n", err)
	tm.Destroy()
}

func TestInterruptHandling(t *testing.T) {
	if os.Getenv("HAND_TEST_SIGNAL_HANDLING") == "" {
		t.Skip("only for hand testing")
	}

	tm := MakeTaskMon()
	var sb strings.Builder
	sb.WriteString("#!/bin/sh\n")
	sb.WriteString("function handle() {\n")
	sb.WriteString("  echo signal caught\n")
	sb.WriteString("  echo foo > /tmp/token.txt\n")
	sb.WriteString("  exit 0\n")
	sb.WriteString("}\n")
	sb.WriteString("trap handle SIGINT\n")
	sb.WriteString("sleep 30\n")
	sb.WriteString("exit 1\n")
	sts := TaskSetup{Name: "A", Command: sb.String()}
	tm.MakeShellTask(sts)
	_, _, err := tm.Kickoff()
	if err != nil {
		t.Errorf("error from kickoff: %v", err)
	}
	tm.Destroy()
}

func TestLonger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping longer test")
	}
	td := t.TempDir()
	if err := testTaskStuff(td); err != nil {
		t.Errorf("testTaskStuff failed with error: %v", err)
	}
}
