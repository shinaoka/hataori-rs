# AGENTS.md

Before acting, read the latest shared tensor4all agent rules from the
[`tensor4all-agent-rules`](https://github.com/tensor4all/tensor4all-agent-rules)
repository online. Start from:

- `https://github.com/tensor4all/tensor4all-agent-rules/blob/main/rules/index.md`

If internet access is unavailable or the remote cannot be resolved, use the
sibling checkout:

- `../tensor4all-agent-rules/rules/index.md`

Load only the common, Rust, performance, numerical, docs, benchmark, or agent
consumer rule files relevant to the task. If neither source is available,
continue from this repository's local rules and state that the shared rules
were unavailable when preparing external-facing work.

Then read the Hataori-specific rules:

- `REPOSITORY_RULES.md`

The shared rules are not vendored here. This repository is expected to move to
the tensor4all GitHub organization in the future, so cross-repository policy
belongs in `tensor4all-agent-rules`; only Hataori-specific durable constraints
belong in this repository.

## Current status

Hataori's synchronous P0 core and P1 bounded-prefetch extension are implemented.
The long-term distributed-runtime architecture is a forward-looking design and
does not describe the current public API. Read both:

- `docs/design.md` for the implemented P0 architecture;
- `docs/design/distributed-runtime.md` for the proposed long-term architecture.

For changes that establish or revise durable architecture, update the design
document and add a concise reviewer-facing work log under `docs/worklogs/`.
