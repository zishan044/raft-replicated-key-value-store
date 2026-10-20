package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/zishan044/raft-replicated-key-value-store.git/pkg/client"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("kvctl", flag.ContinueOnError)
	flags.SetOutput(errOut)
	endpoints := flags.String("endpoints", "127.0.0.1:9000", "comma-separated gRPC endpoints; status uses the first")
	timeout := flags.Duration("timeout", client.DefaultTimeout, "total operation timeout")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(errOut, "usage: kvctl [--endpoints host:port,...] [--timeout 5s] put KEY VALUE|get KEY|del KEY|cas KEY EXPECTED_REVISION VALUE|status")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	args = flags.Args()
	if len(args) == 0 {
		flags.Usage()
		return 2
	}
	count := map[string]int{"put": 3, "get": 2, "del": 2, "cas": 4, "status": 1}
	if want, ok := count[args[0]]; !ok || len(args) != want {
		flags.Usage()
		return 2
	}
	if *timeout <= 0 {
		_, _ = fmt.Fprintln(errOut, "timeout must be positive")
		return 2
	}
	var expected uint64
	if args[0] == "cas" {
		var err error
		expected, err = strconv.ParseUint(args[2], 10, 64)
		if err != nil {
			_, _ = fmt.Fprintln(errOut, "expected revision must be an unsigned integer")
			return 2
		}
	}
	seeds := strings.Split(*endpoints, ",")
	for i := range seeds {
		seeds[i] = strings.TrimSpace(seeds[i])
	}
	c, err := client.New(client.Config{Endpoints: seeds, Timeout: *timeout})
	if err != nil {
		_, _ = fmt.Fprintln(errOut, err)
		return 2
	}
	defer func() { _ = c.Close() }()
	var revision uint64
	switch args[0] {
	case "put":
		revision, err = c.Put(ctx, args[1], []byte(args[2]))
	case "del":
		revision, err = c.Delete(ctx, args[1])
	case "cas":
		revision, err = c.CompareAndSwap(ctx, args[1], expected, []byte(args[3]))
	case "get":
		var gotValue string
		got, getErr := c.Get(ctx, args[1])
		err = getErr
		if err == nil {
			if got.Found {
				gotValue = fmt.Sprintf("value=%q revision=%d\n", got.Value, got.Revision)
			} else {
				gotValue = "found=false\n"
			}
			_, err = fmt.Fprint(out, gotValue)
		}
	case "status":
		st, statusErr := c.Status(ctx, "")
		err = statusErr
		if err == nil {
			_, err = fmt.Fprintf(out, "node_id=%d role=%s term=%d leader_id=%d commit_index=%d applied_index=%d last_log_index=%d\n", st.NodeId, strings.ToLower(strings.TrimPrefix(st.Role.String(), "NODE_ROLE_")), st.Term, st.LeaderId, st.CommitIndex, st.AppliedIndex, st.LastLogIndex)
		}
	}
	if err == nil && (args[0] == "put" || args[0] == "del" || args[0] == "cas") {
		_, err = fmt.Fprintf(out, "revision=%d\n", revision)
	}
	if err != nil {
		_, _ = fmt.Fprintln(errOut, err)
		if errors.Is(err, client.ErrUnknown) {
			_, _ = fmt.Fprintln(errOut, "The write may have applied. This invocation's session is not saved; a new invocation creates a new session.")
		}
		return 1
	}
	return 0
}
