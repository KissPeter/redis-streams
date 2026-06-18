# Release Dependencies - Pinned Versions

This document records the pinned dependencies for the redis-streams release preparation.

## Date
June 18, 2026

## Version
0.3.0 (as per `redis_streams/__init__.py`)

## Main Dependencies (requirements.txt)
- redis==8.0.0
- tabulate==0.10.0

## Development Dependencies (setup.py extras_require['dev'])
- flake8==7.3.0
- black==26.5.1
- mypy==2.1.0
- vulture==2.16
- types-tabulate==0.10.0.20260508
- types-requests==2.33.0.20260518

## Test Dependencies (redis_streams_test/requirements_for_test.txt)
- flake8==7.3.0
- flake8-bugbear==25.11.29
- flake8-bandit==4.1.1
- flake8-black==0.4.0
- flake8-pylint==0.2.1
- flake8-isort==7.0.0
- vulture==2.16
- pylint-json2html==0.5.0
- mypy==2.1.0
- pytest==9.1.0
- pytest-cov==7.1.0
- pytest-html==4.2.0
- pytest-random-order==1.2.0
- pytest-ordering==0.6

## Changes Made
1. ✅ Pinned main dependencies in `requirements.txt`
2. ✅ Pinned dev dependencies in `setup.py`
3. ✅ Pinned test dependencies in `redis_streams_test/requirements_for_test.txt`

## Notes
- All versions are pinned using exact version specifiers (==) for reproducible builds
- These versions were the ones currently installed in the development environment
- This ensures that the package release will have consistent dependency versions

## Next Steps for Release
- Review and test the package with pinned dependencies
- Update CHANGELOG if needed
- Tag the release
- Build and upload to PyPI

