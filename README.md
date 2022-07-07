# The Time Series Refinery

## Installation

Just pip-install it:

```bash
$ pip install tshistory_refinery
```

Create first a postgresql database:

```bash
$ createdb my_time_series
```

Then initialize the database schema:

```bash
$ tsh init-db postgresql:///my_time_series --no-dry-run
```

Last item: we need a configuration file `refinery.cfg` in the home
directory, containing:

```ini
[db]
uri = postgresql:///my_time_series
```
