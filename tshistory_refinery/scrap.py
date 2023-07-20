from hashlib import sha1
from dataclasses import dataclass
import typing
from functools import partial

import pandas as pd

from rework.io import _MOMENT_ENV
from psyl import lisp
from rework import api


@dataclass
class Scrap:
    names: typing.Union[str, typing.Dict[str, str]]
    func: typing.Callable
    schedrule: str
    fromdate: typing.Optional[str] = None
    todate: typing.Optional[str] = None
    initialdate: typing.Optional[str] = None
    precious: typing.Optional[bool] = False

    def name_str(self):
        if isinstance(self.names, str):
            return self.names
        else:
            return "".join(self.names)

    def hash(self):
        identifier = (
            self.name_str() +
            self.schedrule +
            str(self.fromdate) +
            str(self.todate) +
            str(self.initialdate)
        )
        return sha1(
            identifier.encode('utf-8')
        ).hexdigest()

    def scrap_data(self):
        args_dict = {}
        # evaluate the date args (lisp expressions)
        for arg in ['fromdate', 'todate']:
            date_arg = getattr(self, arg)
            if date_arg is None:
                continue
            args_dict[arg] = pd.Timestamp(
                lisp.evaluate(date_arg, env=_MOMENT_ENV)
            )
        data_metadata = self.func(**args_dict)
        return data_metadata

    def func_string(self):
        if isinstance(self.func, partial):
            func_str = f'{self.func.func.__name__}{self.func.args}'
        else:
            func_str = self.func.__name__
        return func_str

    def update_metadata(self, tsa, metadata):
        meta = {'func': self.func_string()}

        if isinstance(self.names, str):
            metadata.update(meta)
            tsa.update_metadata(self.names, metadata)
            return

        for name in self.names:
            if not tsa.exists(name):
                continue
            seriesmeta = metadata.get(name, {})
            seriesmeta.update(meta)
            tsa.update_metadata(name, seriesmeta)

    def update_data(self, tsa, data):
        """
        Data can be a timeseries or a dataframe of timeseries.
        We dispatch the process depending on that.
        """
        if isinstance(self.names, str):
            diff = tsa.update(self.names, data.dropna(), author='scraper')
            if len(diff) > 0:
                return {self.names: 'updated'}
            else:
                return {self.names: 'unchanged'}

        status = {}
        for name in self.names:
            if self.names[name] in data.columns:
                ts = data[self.names[name]]
                diff = tsa.update(name, ts.dropna(), author='scraper')
                status[name] = 'updated' if len(diff) else 'unchanged'
            else:
                print(
                    f'{self.names[name]} not in scraper response. '
                    f'Please check for a possible name change. '
                    f'Available data is :{data.columns}'
                )
                status[name] = 'unavailable'
        return status

    def update(self, tsa):
        data, metadata = self.scrap_data()
        status = self.update_data(tsa, data)
        self.update_metadata(tsa, metadata)
        return status

    def build_inputs(self, task_name):
        inputdata = {
            'identifier': self.hash()
        }

        if isinstance(self.names, str):
            inputdata['seriesname'] = self.names
        else:
            # seriesname is an ui hint, also we don't want
            # to stick every name there ...
            inputdata['seriesname'] = list(self.names.keys())[0]

        if task_name == 'fetch_history':
            date_args = ['initialdate']
        else:
            # refresh
            date_args = ['fromdate', 'todate']

        for date_arg in date_args:
            inputdata[date_arg] = getattr(self, date_arg)
        return inputdata

    def prepare_task(self, engine):
        inputdata = self.build_inputs('refresh')
        api.prepare(
            engine,
            'refresh',
            domain='scrapers',
            rule=self.schedrule,
            inputdata=inputdata
        )


class Scrapers:

    def __init__(self, *scrapers):
        self.scrapers = {
            scrap.hash(): scrap
            for scrap in scrapers
        }

    def merge(self, otherscrapers):
        self.scrapers.update(
            otherscrapers.scrapers
        )

    def __lt__(self, other):
        assert isinstance(other, self.__class__)
        return len(self.scrapers) < len(other.scrapers)

    def find_by_hash(self, key):
        scrap = self.scrapers.get(key)
        if scrap is None:
            raise ValueError(
                f'Unknown hash value: {key}.'
            )
        return scrap

    def seriesnames_inventory(self):
        all_seriesnames = {}
        for id_scr, scr in self.scrapers.items():
            names = scr.names
            if isinstance(names, str):
                all_seriesnames[names] = id_scr
            else:
                for name in list(names.keys()):
                    all_seriesnames[name] = id_scr
        return all_seriesnames

    def find_func_str(self, identifier=None, seriesname=None):
        inventory = self.seriesnames_inventory()
        if identifier is None:
            identifier = inventory[seriesname]
        scrap = self.find_by_hash(identifier)
        func_str = scrap.func_string()
        return func_str

    def seriesname_exists(self, seriesname):
        return seriesname in self.seriesnames_inventory()

    def fetch_history(self, tsa, seriesname, identifier, initialdate, reset):
        inventory = self.seriesnames_inventory()
        if identifier is None:
            identifier = inventory[seriesname]
        if seriesname is None:
            seriesname = {
                v: k
                for k, v in inventory.items()
            }[identifier]

        if not self.seriesname_exists(seriesname):
            raise Exception(
                'This seriesname is not part of scraper inventory.'
            )

        scrap = self.find_by_hash(identifier)
        if scrap.precious:
            raise Exception(
                'This scraper is tagged as precious. Fetch_history '
                'is not possible.'
            )
        if reset:
            tsa.delete(seriesname)

        scrap.fromdate = initialdate
        status = scrap.update(tsa)
        return status

    def refresh(self, tsa, seriesname, identifier, fromdate, todate):
        inventory = self.seriesnames_inventory()
        if identifier is None:
            identifier = inventory[seriesname]
        if seriesname is None:
            seriesname = {
                v: k
                for k, v in inventory.items()
            }[identifier]

            if not self.seriesname_exists(seriesname):
                raise Exception(
                    'This seriesname is not part of scraper inventory.'
                )

        scrap = self.find_by_hash(identifier)
        # we allow an override of fromdate and todate from the task
        if fromdate is not None:
            scrap.fromdate = fromdate
        if todate is not None:
            scrap.todate = todate
        status = scrap.update(tsa)
        return status


# the null scrappers object
SCRAPERS = Scrapers()
