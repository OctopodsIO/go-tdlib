# Octopods fork

Forked from `zelenin/go-tdlib` at `0dd3ea652682`, the commit `telegram-bridge`
was already pinned to. Upstream `master` still carries all three defects below,
so this is not something a version bump fixes.

The bridge depends on this fork through a `replace` in its own `go.mod`, and
keeps importing `github.com/zelenin/go-tdlib/client`. Only `go.mod` here names
the fork.

## The patches

All three are in `client/`, and each one is marked `Octopods patch` in the
source so it survives a merge that does not understand it.

1. **`NewClient` returns the client alongside the error.** Upstream returned
   `nil`. The native TDLib client is already created and holding the session
   database open by then, and nothing outside this package can reach it, so the
   caller had no way to close it. The bridge locks one directory per account,
   and a leak there means the lock is released while TDLib still holds the
   files: the next attempt opens a second client on the same account.

2. **Options are applied before the receiver goroutine starts.** Upstream
   applied them in goroutines afterwards, so `WithResultHandler` wrote
   `client.resultHandler` while `receiver()` read it. A torn read crashes the
   process and takes every other account in it down.

   This breaks `WithProxy`, which sends a request and so cannot complete before
   the receiver exists. The bridge does not use it.

3. **`isClosed` is an `atomic.Bool`.** Upstream wrote it from `receiver()` and
   read it from the shared `tdlibInstance.receiver()`, on another goroutine.

Known and not fixed: the `isClosed` check in `tdlibInstance.receiver()` is still
a check-then-act, so a send can still race a `close(client.responses)`. Making
the flag atomic removes the data race, not that panic.

## Taking an upstream release

    git fetch upstream
    git merge upstream/master

The generated schema files are the bulk of the repo and never conflict. Expect
conflicts only in `go.mod`, `client/client.go`, and `client/tdlib.go`. Keep the
`Octopods patch` blocks and re-read this file before resolving.
