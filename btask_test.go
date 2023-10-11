// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package btask

import (
	"path/filepath"
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

func TestLonger(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping longer test")
	}
	td := t.TempDir()
	if err := testTaskStuff(td); err != nil {
		t.Errorf("testTaskStuff failed with error: %v", err)
	}
}
