## Summary

Describe the Jepsen workload, checker, nemesis, or harness changes.

## Related Issues

Closes #

## Verification

- [ ] `lein test`
- [ ] `lein run test --help`
- [ ] `shellcheck docker/entrypoint.sh scripts/*.sh`
- [ ] Docker compose validation (`docker compose -f docker/docker-compose.4x4.yml config -q`)
