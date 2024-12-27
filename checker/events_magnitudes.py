import operator


def check_presence(item, mydict):
    """
    Checks for the presence of item in the values of dictionary mydict, where the values are lists.

        Parameters:
            item (string): String to search for within the dictionary
            mydict (dict): Dictionary of key-value pairs, where the values are lists of strings

        Returns:
            [item_in_dict, dict_key] (list): List containing two items: Boolean value item_in_dict
            stating whether item was found in mydict, and string dict_key, which is the key associated
            with the value containing item (if item is not present, dict_key == None)
    """
    # If item is not found, values of item_in_dict and dict_key will return as False and None
    item_in_dict = False
    dict_key = None
    # Iterate through dictionary values
    for i in range(len(mydict.values())):
        # If item is present in the i-th value, set the i-th key as dict_key and change item_in_dict to True
        if item in list(mydict.values())[i]:
            dict_key = list(mydict.keys())[i]
            item_in_dict = True
    return [item_in_dict, dict_key]


# Map names of CASA/NOAA alert types as they appear in Event Types menu to their names in the MySQL database
casa_events_dict = {
    "CASA Hail": "HAIL_CASA",
    "CASA Rain Rate": "RAIN_RATE_CASA",
    "NWS Severe Thunderstorm Warning": "SVR_WARNING_NWS",
    "CASA 15-min Rain Accumulation": "RAIN_ACCUM_CASA_15",
    "NWS Flood Warning": "FL_WARNING_NWS",
    "CASA Basin Alert": "BASIN_ALERT",
    "NWS Tornado Warning": "TOR_WARNING_NWS",
    "NWS Flash Flood Warning": "FF_WARNING_NWS",
}

# Map CASA/NOAA alert names to shorter versions to use in HTML classes/IDs
casa_events_url = {
    "CASA Hail": "hail",
    "CASA Rain Rate": "rr",
    "NWS Severe Thunderstorm Warning": "svr",
    "CASA 15-min Rain Accumulation": "rain",
    "NWS Flood Warning": "flood",
    "CASA Basin Alert": "basin",
    "NWS Tornado Warning": "tornado",
    "NWS Flash Flood Warning": "ff",
}

# Map CASA/NOAA alert names to shorter versions for display in query results
casa_events_display = {
    "CASA Hail": "CASA Hail",
    "CASA Rain Rate": "CASA Rain Rate",
    "NWS Severe Thunderstorm Warning": "NWS-SVR",
    "CASA 15-min Rain Accumulation": "CASA 15 min Rain",
    "NWS Flood Warning": "NWS-FW",
    "CASA Basin Alert": "CASA Basin Alert",
    "NWS Tornado Warning": "NWS-TOR",
    "NWS Flash Flood Warning": "NWS-FFW",
}

# Dictionary associating storm types with relevant magnitudes
storm_types_and_magnitudes = {
    "Tornado": ["Any Tornado", "EF0", "EF1", "EF2", "EF3", "EF4", "EF5"],
    "Hail": ["Any Hail", "< 1 in", "1-2 in", "2-3 in", "3+ in"],
    "High Wind": ["Any Wind", "< 30 mph", "30-58 mph", "58-70 mph", "70+ mph"],
    "Strong Wind": ["Any Wind", "< 30 mph", "30-58 mph", "58-70 mph", "70+ mph"],
    "Thunderstorm Wind": ["Any Wind", "< 30 mph", "30-58 mph", "58-70 mph", "70+ mph"],
    "Blizzard": [],
    "Flash Flood": [],
    "Flood": [],
    "Funnel Cloud": [],
    "Heavy Rain": [],
    "Heavy Snow": [],
    "Ice Storm": [],
}

# Add CASA event names to storm_types_and_magnitudes with [] as the value (since they are not associated with magnitudes)
for item in casa_events_dict.keys():
    storm_types_and_magnitudes[item] = []

# Categories used for displaying events together in query results
event_categories = {
    "Flooding Events": [
        "Flash Flood",
        "Flood",
        "Heavy Rain",
        "NWS Flood Warning",
        "NWS Flash Flood Warning",
        "CASA Basin Alert",
        "CASA Rain Rate",
        "CASA 15-min Rain Accumulation",
    ],
    "Convective Storm Events": [
        "Funnel Cloud",
        "Hail",
        "High Wind",
        "Strong Wind",
        "Thunderstorm Wind",
        "Tornado",
        "Heavy Rain",
        "CASA Hail",
        "NWS Severe Thunderstorm Warning",
        "NWS Tornado Warning",
    ],
    "Winter Storm Events": ["Blizzard", "Heavy Snow", "Ice Storm"],
}

# Groupings of events that share magnitudes
same_magnitudes = {"Wind": ["High Wind", "Strong Wind", "Thunderstorm Wind"]}

# Associates magnitudes with operations and the data that's being compared to data magnitude
comparisons = {
    "Any Hail": [(">", 0)],
    "< 1 in": [("<", 1)],
    "1-2 in": [(">=", 1), ("<", 2)],
    "2-3 in": [(">=", 2), ("<", 3)],
    "3+ in": [(">=", 3)],
    "Any Wind": [(">", 0)],
    "< 30 mph": [("<", 30)],
    "30-58 mph": [(">=", 30), ("<", 58)],
    "58-70 mph": [(">=", 58), ("<", 70)],
    "70+ mph": [(">=", 70)],
    "Any Tornado": [("in", "EF")],
    "EF0": [("==", "EF0")],
    "EF1": [("==", "EF1")],
    "EF2": [("==", "EF2")],
    "EF3": [("==", "EF3")],
    "EF4": [("==", "EF4")],
    "EF5": [("==", "EF5")],
}

# Associate operations from comparisons dictionary with operator functions
ops = {
    "<": operator.lt,
    "<=": operator.le,
    ">": operator.gt,
    ">=": operator.ge,
    "==": operator.eq,
    "in": operator.contains,
}

# Numbers to multiply int/float magnitudes by
# Wind events are multiplied by 1.15078 to convert speed in knots to MPH
event_magnitudes_multipliers = {
    "Hail": 1,
    "High Wind": 1.15078,
    "Strong Wind": 1.15078,
    "Thunderstorm Wind": 1.15078,
}

# Units associated with magnitudes
magnitude_units = {
    "Hail": "in",
    "High Wind": "mph",
    "Strong Wind": "mph",
    "Thunderstorm Wind": "mph",
    "Tornado": None,
}

# List of relevant counties
# There are other possible lists of counties to use - we decided to use the most inclusive one
countieslist = [
    "Dallas",
    "Denton",
    "Ellis",
    "Johnson",
    "Tarrant",
    "Wise",
    "Collin",
    "Somervell",
    "Rockwall",
    "Hood",
    "Parker",
    "Hill",
    "Navarro",
    "Kaufman",
    "Montague",
    "Cooke",
    "Grayson",
    "Bosque",
    "Jack",
    "Fannin",
    "Hunt",
    "Henderson",
    "Limestone",
    "Clay",
    "Erath",
    "Freestone",
    "Hamilton",
    "McLennan",
]
# Sort countieslist alphabetically
countieslist.sort()

# Create tuples containing an integer and county name - used to display counties in HTML form
counties_tuple = [(0, "All")] + [(i, k) for i, k in enumerate(countieslist, 1)]

# Convert county names to uppercase for comparison with database
countieslist = [item.upper() for item in countieslist]

# Create dictionary of counties
counties_dict = {i: k for i, k in enumerate(countieslist, 1)}
counties_dict[0] = "All"

# List of storm types from storm_types_and_magnitudes in alphabetical order
storm_types_sorted = list(storm_types_and_magnitudes.keys()) + ["All"]
storm_types_sorted.sort()

# Event categories for simplified menu - multiple NOAA/CASA event types map to each of these
menu_categories = [
    "All",
    "Flash Flood",
    "Flood",
    "Hail",
    "Heavy Rain",
    "High Wind",
    "Severe Weather",
    "Tornado",
    "Winter Weather",
]

# Dictionary mapping numbers to event categories
categories_dict = {i: k for i, k in enumerate(menu_categories)}

# Dictionary mapping number associated with each event type to that event type
events_dict = {
    i: k for i, k in enumerate(storm_types_sorted, start=len(menu_categories))
}

# Create tuples of event categories for displaying on HTML/Flask form
category_tuples = [(i, k) for i, k in enumerate(menu_categories)]

# Map menu category types to list of associated events that will be displayed when category is selected
categories_eventtypes_map = {
    "Flash Flood": ["Flash Flood", "NWS Flash Flood Warning"],
    "Flood": ["Flood", "NWS Flood Warning"],
    "Hail": ["Hail", "CASA Hail"],
    "Heavy Rain": [
        "Heavy Rain",
        "CASA Rain Rate",
        "CASA 15-min Rain Accumulation",
        "CASA Basin Alert",
    ],
    "High Wind": ["High Wind", "Strong Wind", "Thunderstorm Wind"],
    "Severe Weather": [
        "NWS Severe Thunderstorm Warning",
        "Thunderstorm Wind",
        "CASA Hail",
    ],
    "Tornado": ["Tornado", "Funnel Cloud", "NWS Tornado Warning"],
    "Winter Weather": ["Ice Storm", "Blizzard", "Heavy Snow"],
}
categories_eventtypes_map["All"] = [
    x for y in categories_eventtypes_map.values() for x in y
]

# Alphabetize events associated with each category
for item in categories_eventtypes_map.values():
    item.sort()

# Map events to associated magnitudes (only including events with associated magnitudes)
magnitudes_dict_notuples = {}
# Loop through event types
for key in storm_types_and_magnitudes:
    # If there are magnitudes associated with this event
    if storm_types_and_magnitudes[key] != []:
        # Check if this event is in same_magnitudes list (whether it shares its magnitudes with any other event type)
        item_in_samemagnitudes = check_presence(key, same_magnitudes)
        # If event type is not in same_magnitudes, add it to the dictionary with the list of its associated magnitudes as the value
        if item_in_samemagnitudes[0] == False:
            magnitudes_dict_notuples[key] = storm_types_and_magnitudes[key]
        else:  # If event type is in same_magnitudes
            # If magnitudes aren't in the dictionary yet, add them
            if storm_types_and_magnitudes[key] not in list(
                magnitudes_dict_notuples.values()
            ):
                magnitudes_dict_notuples[item_in_samemagnitudes[1]] = (
                    storm_types_and_magnitudes[key]
                )

# Will be used to display magnitude types on the HTML/Flask form
magnitudes_tuples_dict = {}
# Iterate through dictionary and create tuples
# Counter determines the number to be included in the tuple
counter = 0
# Loop through dictionary keys (event types)
for key in magnitudes_dict_notuples:
    # Create a blank list to add tuples to
    value_with_tuples = []
    # Loop through magnitude types in value associated with key
    for magnitude in magnitudes_dict_notuples[key]:
        # Create a tuple consisting of the value of counter and the magnitude
        magnitude_tuple = (counter, magnitude)
        # Add it to the list of values with tuples
        value_with_tuples.append(magnitude_tuple)
        # Increment counter for next tuple
        counter += 1
    # Replace the value in the dictionary with the new value containing tuples
    magnitudes_tuples_dict[key] = value_with_tuples

# Create list of magnitudes
magnitudes_list = []
for innerlist in list(magnitudes_dict_notuples.values()):
    for item in innerlist:
        magnitudes_list.append(item)

# Create a dictionary mapping a number to each magnitude in the list of magnitudes
magnitudes_dict = {i: v for i, v in enumerate(magnitudes_list)}
