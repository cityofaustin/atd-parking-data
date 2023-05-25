"""
Location names lookup for parking meter IDs based on a range of IDs.
New parking meters that are added to these location groups should be within
these ranges.
"""
# Adding one to the ending ID to make this range inclusive of the final ID given

METER_LOCATION_NAMES = {
    range(19001905, 19001909 + 1): "MoPac Lot",
    range(91001901, 91001904 + 1): "MACC Lot",
    range(81002101, 81002103 + 1): "Butler Shores",
    range(81002104, 81002104 + 1): "Walsh",
    range(81002105, 81002105 + 1): "Walsh",
    range(44004401, 44004401 + 1): "Woods of Westlake",
    range(21002112, 21002113 + 1): "Dawson's Lot",
    range(10000101, 10000899 + 1): "Core",
    range(10001001, 10001099 + 1): "Core",
    range(10001201, 10001299 + 1): "Core",
    range(20000901, 20000999 + 1): "Non-Core",
    range(20001101, 20001199 + 1): "Non-Core",
    range(20001301, 20001499 + 1): "Non-Core",
    range(20001701, 20001799 + 1): "Non-Core",
    range(23002301, 23002399 + 1): "Non-Core",
    range(71002001, 71002099 + 1): "Toomey",
    range(61001601, 61001699 + 1): "Rainey",
    range(35001501, 35001599 + 1): "West Campus",
    range(35002501, 35002599 + 1): "West Campus",
    range(24002401, 24002499 + 1): "East Austin",
    range(26002601, 26002699 + 1): "Mueller",
    range(27002701, 27002799 + 1): "Colorado River",
    range(28002801, 28002899 + 1): "Austin High",
    range(81002119, 81002120 + 1): "Bartholomew Pool",
    range(36730001, 36730009 + 1): "Barton Springs Pool",
    range(81002121, 81002122 + 1): "Deep Eddy Pool",
    range(81002106, 81002107 + 1): "Emma",
    range(81002123, 81002123 + 1): "Garrison Pool",
    range(81002125, 81002125 + 1): "Hancock Golf Course",
    range(81002126, 81002134 + 1): "Barton Springs Pool",  # New kiosks for 2023
    range(81002124, 81002124 + 1): "Northwest Pool",
    range(81002108, 81002110 + 1): "Walter",
    range(82002201, 82002299 + 1): "Zilker",
}

# For going between passport app zones and zone names
APP_LOCATION_NAMES = {
    range(39001, 39001 + 1): "IH 35 Lot",
    range(39002, 39002 + 1): "MoPac Lot",
    range(39003, 39003 + 1): "MACC Lot",
    range(39004, 39004 + 1): "Butler Shores",
    range(39005, 39005 + 1): "Walsh",
    range(39006, 39006 + 1): "Walsh",
    range(39008, 39013 + 1): "Woods of Westlake",
    range(39014, 39022 + 1): "Q2 Stadium",
    range(39024, 39024 + 1): "Dawson's Lot",
    range(39025, 39026 + 1): "Silicon and Titanium",
    range(39100, 39399 + 1): "Core",
    range(39400, 39559 + 1): "Non-Core",
    range(39588, 39599 + 1): "Non-Core",
    range(39560, 39570 + 1): "Toomey",
    range(39573, 39587 + 1): "Rainey",
    range(39600, 39699 + 1): "West Campus",
    range(39700, 39799 + 1): "East Austin",
    range(39800, 39899 + 1): "Mueller",
    range(39900, 39901 + 1): "Colorado River",
    range(39903, 39905 + 1): "Colorado River",
    range(39902, 39902 + 1): "Austin High",
    range(39007, 39007 + 1): "Austin High",
}
