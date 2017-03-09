#!/usr/bin/env python
#  -*- coding: utf-8 -*-

from collections import defaultdict
from datetime import datetime, timedelta
import os
import pandas as pd


OUTPUT_CNT = 100             # None or +integer ; how many series we want generate
# OUTPUT_CNT = 999999999       # generate all
FLIGHTS_AND_COUNTRIES = 10   # count of flights (and countries) in single series
MIN_HOURS_STAY = 3           # minimal time for change


BEFORE_RETURN_IDX = FLIGHTS_AND_COUNTRIES - 2   # with this index we limit flights into "returnable" airports (see bellow) only

DIR_ROOT = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
DIR_TASK = 'zadani'
FILE_TASK = 'input_data.csv'
DIR_CODES = 'codes'
FILE_CODES = 'airports.csv'  # http://ourairports.com/data/  gives missing: PEK ONK AGM FLZ KUZ MLH ZGS PLX AOQ SGN TWT LPF THD
'''missing, partially found on https://www.world-airport-codes.com or similar
PEK CN, Beijing Capital International
ONK RU, Olenek
AGM GL, Tasiilaq Airport
FLZ ID, Sibolga Ferdinand LumbanTobing
KUZ KR, Gunsan Airbase
MLH FR, EuroAirport
ZGS CA, Gethsemani Airport
PLX KZ, Semipalatinsk
AOQ GL, Aappilattoq Heliport
SGN VN, Tan Son Nhat International Airport
TWT PH, Tawitawi Airport
LPF CN, Liupanshui Yue Zhao
THD VN, Thanh Hoa Tho Xuan
'''
STRANGE_CODES = {'PEK': 'CN', 'ONK': 'RU', 'AGM': 'GL', 'FLZ': 'ID',   # not contained in FILE_CODES
                 'KUZ': 'KR', 'MLH': 'FR', 'ZGS': 'CA', 'PLX': 'KZ',
                 'AOQ': 'GL', 'SGN': 'VN', 'TWT': 'PH', 'LPF': 'CN',
                 'THD': 'VN'}


def go():
    # time0 = datetime.now()
    generate_required_count(*prepare_data())
    # print datetime.now() - time0       # exec duration

def prepare_data():
    print 'preparing flights data ...'
    pflights = pd.read_csv(os.path.join(DIR_ROOT, DIR_TASK, FILE_TASK), sep=';')
    pairports = pd.read_csv(os.path.join(DIR_ROOT, DIR_CODES, FILE_CODES))

    codes = STRANGE_CODES
    missing_iata = set()
    flights = []
    airports = defaultdict(lambda: [[], set(), 0])  # outgoing flights, "returnable" airports, starting resolve positions

    for _idx, airport in pairports.iterrows():
        if airport.iata_code:
            codes[airport.iata_code] = airport.iso_country
    for _idx, flight in pflights.iterrows():
        if flight.source not in codes:
            missing_iata.add(flight.source)
            continue
        if flight.destination not in codes:
            missing_iata.add(flight.destination)
            continue
        flights.append({
            'sc': codes[flight.source],        # source country
            'dc': codes[flight.destination],   # destination country
            'sa': flight.source,               # source airport
            'da': flight.destination,          # destination airport
            'dep': datetime.strptime(flight.local_departure_time, '%Y-%m-%d %H:%M:%S'),
            'arr': datetime.strptime(flight.local_arrival_time, '%Y-%m-%d %H:%M:%S'),
        })

    flights = sorted(flights, key=lambda flight: flight['dep'])
    for idx, flight in enumerate(flights):
        flight['idx'] = idx                           # idx to find identical rows between flights and airports[0]
        airports[flight['sa']][0].append(flight)      # add outgoing flight (to the source)
        airports[flight['da']][1].add(flight['sa'])   # add "returnable" airport (to the destination)

    print ' ... done.'
    print
    if missing_iata:
        print 'missing airport codes:', ' '.join(missing_iata)
        print

    return flights, airports
    # airports .. dict of airports, key is airport iata code
    #   airport[0] .. outgoing flights
    #   airport[1] .. "returnable" airports      # seems this doesn't help much at least for small count of series
    #   airport[2] .. starting flight pos to resolve (pos before are sure the older flights)
    # we provide here 2 copies of flights:
    #   1: flights, sorted by date
    #   2: airport[0], sorted by date, filtered (grouped) for outgoing airport


def generate_required_count(flights, airports):
    out_no = 0
    # here we cycle through date sorted flights,
    #   so we can move the airports[2] position pointer to skip older flights in airports[0] (~ airport outgoing flights)
    for init_pos, flight1 in enumerate(flights):
        if flight1['sc'] != flight1['dc']:    # is international
            out_no, stop = generate_based_on_initial_flight(flights, airports, flight1, out_no)
            if stop:
                break


def generate_based_on_initial_flight(flights, airports, flight1, out_no):
    origin = flight1['sa']
    last_date = flight1['dep'] + timedelta(days=366)   # 1 year; but +24 hours is allowed
    flight_ids = [flight1['idx']]
    countries = [flight1['sc']]
    flight_airport = flight1['da']
    flight_country = flight1['dc']
    flight_departure = allowed_departure(flight1)
    flight_pos_after = flight1['idx']
    # for the initial flight find all possibilities
    return recursive_generate(flights, airports, flight_ids, countries, flight_airport, flight_country, flight_departure, flight_pos_after, origin, last_date, out_no)


def recursive_generate(flights, airports, flight_ids, countries, flight_airport, flight_country, flight_departure, flight_pos_after, origin, last_date, out_no):
    from_idx = airports[flight_airport][2]
    flights_to_resolve = airports[flight_airport][0][from_idx:]   # slicing: skip flights which are sure older
    last_idx_can_be_skipped = -1
    chain_len = len(flight_ids)
    for idx, flight in enumerate(flights_to_resolve):
        if flight['idx'] <= flight_pos_after:     # isn't later as the initial flight -> is sure earlier
            last_idx_can_be_skipped = idx
        elif flight['dep'] >= flight_departure:   # is later
            if flight['arr'] >= last_date:   # however this is to late ...
                break                        # ... and all remaining are more late
            if chain_len > BEFORE_RETURN_IDX:                                  # accept origin airport only
                if flight['da'] != origin:
                    continue
            elif flight['dc'] in countries or flight['dc'] == flight_country:  # accept new country only
                continue
            if chain_len == BEFORE_RETURN_IDX and flight_airport not in airports[origin][1]:  # accept "returnable" airports only
                continue   # seems this doesn't help much at least for small count of series

            flight_ids.append(flight['idx'])
            countries.append(flight['sc'])
            if len(flight_ids) >= FLIGHTS_AND_COUNTRIES:
                out_no = report(flight_ids, flights, out_no)
                if out_no >= OUTPUT_CNT:
                    return None, True  # stop all recursive calls
            else:
                flight_departure = allowed_departure(flight)
                out_no, stop = recursive_generate(flights, airports, flight_ids, countries, flight['da'], flight['dc'], flight_departure, flight_pos_after, origin, last_date, out_no)
                if stop:
                    return None, True  # stop all recursive calls
            flight_ids.pop(-1)
            countries.pop(-1)
    airports[flight_airport][2] = from_idx + last_idx_can_be_skipped + 1  # if we go again back to this airport, we can ignore flights before this position - they will sure be older as required
    return out_no, False


def allowed_departure(previous_flight):
    return previous_flight['arr'] + timedelta(hours=MIN_HOURS_STAY)


def report(flight_ids, flights, out_no):
    assert len(flight_ids) == FLIGHTS_AND_COUNTRIES
    out_no += 1
    for idx in flight_ids:
        flight = flights[idx]
        print '{};{};{};{};{};{}'.format(out_no, flight['sc'], flight['sa'], flight['da'],
                                         flight['dep'].isoformat()[:16], flight['arr'].isoformat()[:16])
    return out_no


if __name__ == '__main__':
    go()
