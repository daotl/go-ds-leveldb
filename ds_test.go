// Copyright for portions of this fork are held by [Jeromy Johnson, 2016] as
// part of the original go-ds-leveldb project. All other copyright for
// this fork are held by [The BDWare Authors, 2020]. All rights reserved.
// Use of this source code is governed by MIT license that can be
// found in the LICENSE file.

package leveldb

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"testing"

	ds "github.com/bdware/go-datastore"
	key "github.com/bdware/go-datastore/key"
	dsq "github.com/bdware/go-datastore/query"
	dstest "github.com/bdware/go-datastore/test"
)

var testcases = map[string]string{
	"/a":     "a",
	"/a/b":   "ab",
	"/a/b/c": "abc",
	"/a/b/d": "a/b/d",
	"/a/c":   "ac",
	"/a/d":   "ad",
	"/e":     "e",
	"/f":     "f",
}

// returns datastore, and a function to call on exit.
// (this garbage collects). So:
//
//  d, close := newDS(t)
//  defer close()
func newDS(t *testing.T) (*Datastore, func()) {
	path, err := ioutil.TempDir("", "testing_leveldb_")
	if err != nil {
		t.Fatal(err)
	}

	d, err := NewDatastore(path, key.KeyTypeString, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d, func() {
		os.RemoveAll(path)
		d.Close()
	}
}

// newDSMem returns an in-memory datastore.
func newDSMem(t *testing.T) *Datastore {
	d, err := NewDatastore("", key.KeyTypeString, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func addTestCases(t *testing.T, d *Datastore, testcases map[string]string) {
	for k, v := range testcases {
		dsk := key.NewStrKey(k)
		if err := d.Put(dsk, []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	for k, v := range testcases {
		dsk := key.NewStrKey(k)
		v2, err := d.Get(dsk)
		if err != nil {
			t.Fatal(err)
		}
		if string(v2) != v {
			t.Errorf("%s values differ: %s != %s", k, v, v2)
		}
	}

}

func testQuery(t *testing.T, d *Datastore) {
	addTestCases(t, d, testcases)

	rs, err := d.Query(dsq.Query{Prefix: key.QueryStrKey("/a/")})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, key.StrsToKeys([]string{
		"/a/b",
		"/a/b/c",
		"/a/b/d",
		"/a/c",
		"/a/d",
	}), rs)

	// test offset and limit

	rs, err = d.Query(dsq.Query{Prefix: key.QueryStrKey("/a/"), Offset: 2, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}

	expectMatches(t, key.StrsToKeys([]string{
		"/a/b/d",
		"/a/c",
	}), rs)

	// test order

	rs, err = d.Query(dsq.Query{Orders: []dsq.Order{dsq.OrderByKey{}}})
	if err != nil {
		t.Fatal(err)
	}

	keys := make([]string, 0, len(testcases))
	for k := range testcases {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	expectOrderedMatches(t, key.StrsToKeys(keys), rs)

	rs, err = d.Query(dsq.Query{Orders: []dsq.Order{dsq.OrderByKeyDescending{}}})
	if err != nil {
		t.Fatal(err)
	}

	// reverse
	for i, j := 0, len(keys)-1; i < j; i, j = i+1, j-1 {
		keys[i], keys[j] = keys[j], keys[i]
	}

	expectOrderedMatches(t, key.StrsToKeys(keys), rs)
}

func TestQuery(t *testing.T) {
	d, close := newDS(t)
	defer close()
	testQuery(t, d)
}
func TestQueryMem(t *testing.T) {
	d := newDSMem(t)
	testQuery(t, d)
}

func TestQueryRespectsProcess(t *testing.T) {
	d, close := newDS(t)
	defer close()
	addTestCases(t, d, testcases)
}

func TestCloseRace(t *testing.T) {
	d, close := newDS(t)
	for n := 0; n < 100; n++ {
		d.Put(key.NewStrKey(fmt.Sprintf("%d", n)), []byte(fmt.Sprintf("test%d", n)))
	}

	tx, _ := d.NewTransaction(false)
	tx.Put(key.NewStrKey("txnversion"), []byte("bump"))

	closeCh := make(chan interface{})

	go func() {
		close()
		closeCh <- nil
	}()
	for k := range testcases {
		tx.Get(key.NewStrKey(k))
	}
	tx.Commit()
	<-closeCh
}

func TestCloseSafety(t *testing.T) {
	d, close := newDS(t)
	addTestCases(t, d, testcases)

	tx, _ := d.NewTransaction(false)
	err := tx.Put(key.NewStrKey("test"), []byte("test"))
	if err != nil {
		t.Error("Failed to put in a txn.")
	}
	close()
	err = tx.Commit()
	if err == nil {
		t.Error("committing after close should fail.")
	}
}

func TestQueryRespectsProcessMem(t *testing.T) {
	d := newDSMem(t)
	addTestCases(t, d, testcases)
}

func expectMatches(t *testing.T, expect []key.Key, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for _, k := range expect {
		found := false
		for _, e := range actual {
			if e.Key.Equal(k) {
				found = true
			}
		}
		if !found {
			t.Error(k, "not found")
		}
	}
}

func expectOrderedMatches(t *testing.T, expect []key.Key, actualR dsq.Results) {
	t.Helper()
	actual, err := actualR.Rest()
	if err != nil {
		t.Error(err)
	}

	if len(actual) != len(expect) {
		t.Error("not enough", expect, actual)
	}
	for i := range expect {
		if !expect[i].Equal(actual[i].Key) {
			t.Errorf("expected %q, got %q", expect[i], actual[i].Key)
		}
	}
}

func testBatching(t *testing.T, d *Datastore) {
	b, err := d.Batch()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		err := b.Put(key.NewStrKey(k), []byte(v))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = b.Commit()
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range testcases {
		val, err := d.Get(key.NewStrKey(k))
		if err != nil {
			t.Fatal(err)
		}

		if v != string(val) {
			t.Fatal("got wrong data!")
		}
	}
}

func TestBatching(t *testing.T) {
	d, done := newDS(t)
	defer done()
	testBatching(t, d)
}

func TestBatchingMem(t *testing.T) {
	d := newDSMem(t)
	testBatching(t, d)
}

func TestDiskUsage(t *testing.T) {
	d, done := newDS(t)
	addTestCases(t, d, testcases)
	du, err := d.DiskUsage()
	if err != nil {
		t.Fatal(err)
	}

	if du == 0 {
		t.Fatal("expected some disk usage")
	}

	k := key.NewStrKey("more")
	err = d.Put(k, []byte("value"))
	if err != nil {
		t.Fatal(err)
	}

	du2, err := d.DiskUsage()
	if du2 <= du {
		t.Fatal("size should have increased")
	}

	done()

	// This should fail
	_, err = d.DiskUsage()
	if err == nil {
		t.Fatal("DiskUsage should fail when we cannot walk path")
	}
}

func TestDiskUsageInMem(t *testing.T) {
	d := newDSMem(t)
	du, _ := d.DiskUsage()
	if du != 0 {
		t.Fatal("inmem dbs have 0 disk usage")
	}
}

func TestTransactionCommit(t *testing.T) {
	key := key.NewStrKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	if err := txn.Put(key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if err := txn.Commit(); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != nil || !bytes.Equal(val, []byte("hello")) {
		t.Fatalf("expected entry present after commit, got err: %v, value: %v", err, val)
	}
}

func TestTransactionDiscard(t *testing.T) {
	key := key.NewStrKey("/test/key1")

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	if err := txn.Put(key, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
	if txn.Discard(); err != nil {
		t.Fatal(err)
	}
	if val, err := d.Get(key); err != ds.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got err: %v, value: %v", err, val)
	}
}

func TestTransactionManyOperations(t *testing.T) {
	keys := []key.Key{key.NewStrKey("/test/key1"), key.NewStrKey("/test/key2"), key.NewStrKey("/test/key3"), key.NewStrKey("/test/key4"), key.NewStrKey("/test/key5")}

	d, done := newDS(t)
	defer done()

	txn, err := d.NewTransaction(false)
	if err != nil {
		t.Fatal(err)
	}
	defer txn.Discard()

	// Insert all entries.
	for i := 0; i < 5; i++ {
		if err := txn.Put(keys[i], []byte(fmt.Sprintf("hello%d", i))); err != nil {
			t.Fatal(err)
		}
	}

	// Remove the third entry.
	if err := txn.Delete(keys[2]); err != nil {
		t.Fatal(err)
	}

	// Check existences.
	if has, err := txn.Has(keys[1]); err != nil || !has {
		t.Fatalf("expected key[1] to be present, err: %v, has: %v", err, has)
	}
	if has, err := txn.Has(keys[2]); err != nil || has {
		t.Fatalf("expected key[2] to be absent, err: %v, has: %v", err, has)
	}

	var res dsq.Results
	if res, err = txn.Query(dsq.Query{Prefix: key.QueryStrKey("/test")}); err != nil {
		t.Fatalf("query failed, err: %v", err)
	}
	if entries, err := res.Rest(); err != nil || len(entries) != 4 {
		t.Fatalf("query failed or contained unexpected number of entries, err: %v, results: %v", err, entries)
	}

	txn.Discard()
}

func TestSuite(t *testing.T) {
	d := newDSMem(t)
	defer d.Close()
	dstest.SubtestAll(t, d)
}
