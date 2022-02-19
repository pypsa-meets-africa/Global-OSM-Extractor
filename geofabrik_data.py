# Geofabrik and iso norm deviate for some countries and domains

# dictionary of correspondance between iso country codes and geofabrik codes containing those information
# This dictionary instructs the script download_osm_data about how to successfully download data
# from countries that are aggregated into osm.
# For example, Senegal (SN) and Gambia (GM) cannot be downloaded from OSM separately, but only jointly as SNGM
#   That's the reason why in this dictionary they can be found the following entries:
#       "SN": "SNGM"
#       "GM": "SNGM"
#   This instruct the workflow that when the country "SN" is requested, then it shall download the "SNGM" file
iso_to_geofk_dict = {
    "EH": "MA",  # Western Sahara -> Morocco
    "SN": "SNGM",  # Senegal -> Senegal-Gambia
    "GM": "SNGM",  # Gambia -> Senegal-Gambia
    "HK": "CN",  # Hong Kong  -> China
    "MO": "CN",  # Macao  -> China
    "SG": "MY",  # Singapore -> Malaysia
    "BN": "MY",  # Brunei -> Malaysia
    "SA": "GCC",  # Saudi Arabia -> Gulf Cooperation Council
    "KW": "GCC",  # Kuwait -> Gulf Cooperation Council
    "BH": "GCC",  # Bahrain -> Gulf Cooperation Council
    "QA": "GCC",  # Qatar -> Gulf Cooperation Council
    "AE": "GCC",  # United Arab Emirates -> Gulf Cooperation Council
    "OM": "GCC",  # Oman -> Gulf Cooperation Council
}

# Cyprus and Georgia -> European domain
# Russia -> a separate domain

# data for some islands seem to be merged with some other areas data
# "FO": "faroe islands"
# "NF": "norfolk island",
# "PF": "french-polynesia"
# "GU": "guam"

# "latin_america" -> "south-america"

world_geofk = {
    "africa": {
        "DZ": "algeria",
        "AO": "angola",
        "BJ": "benin",
        "BW": "botswana",
        "BF": "burkina-faso",
        "BI": "burundi",
        "CM": "cameroon",
        # canary-islands, # Island
        # "CV": "cape-verde", # Island
        "CF": "central-african-republic",
        "TD": "chad",
        # "KM": "comores", # Island
        "CG": "congo-brazzaville",
        "CD": "congo-democratic-republic",
        "DJ": "djibouti",
        "EG": "egypt",
        "GQ": "equatorial-guinea",
        "ER": "eritrea",
        "ET": "ethiopia",
        "GA": "gabon",
        "GH": "ghana",
        "GW": "guinea-bissau",  # No Data
        "GN": "guinea",
        "CI": "ivory-coast",
        "KE": "kenya",
        "LS": "lesotho",
        "LR": "liberia",
        "LY": "libya",
        "MG": "madagascar",
        "MW": "malawi",
        "ML": "mali",
        "MR": "mauritania",
        # "MU": "mauritius", # Island
        "MA": "morocco",
        "MZ": "mozambique",
        "NA": "namibia",
        "NE": "niger",
        "NG": "nigeria",
        "RW": "rwanda",
        # saint-helena-ascension-and-tristan-da-cunha # Islands
        # "ST": "sao-tome-and-principe", # Island
        "SNGM": "senegal-and-gambia",  # Geofk shortcurt
        # "SC": "seychelles", # Island
        "SL": "sierra-leone",
        "SO": "somalia",  # No Data
        # south-africa-and-lesotho
        "ZA": "south-africa",
        "SS": "south-sudan",
        "SD": "sudan",
        "SZ": "swaziland",
        "TZ": "tanzania",
        "TG": "togo",
        "TN": "tunisia",
        "UG": "uganda",
        "ZM": "zambia",
        "ZW": "zimbabwe",
    },
    "asia": {
        "AF": "afghanistan",
        "AM": "armenia",
        "AZ": "azerbaijan",
        "BD": "bangladesh",
        "BT": "bhutan",
        # "IO": "british indian ocean territory", # Island
        "KH": "cambodia",
        "CN": "china",
        # "CX": "christmas island", # Island
        # "CC": "cocos (keeling) islands", # Island
        "GCC": "gcc-states",  # Geofk shortcurt for SA, KW, BH, QA, AE, OM
        "IN": "india",
        "ID": "indonesia",
        "IR": "iran",
        "IQ": "iraq",
        "IL-PL": "israel-and-palestine",
        "JP": "japan",
        "JO": "jordan",
        "KZ": "kazakhstan",
        "KP": "north-korea",
        "KR": "south-korea",
        "KG": "kyrgyzstan",
        "LA": "laos",
        "LB": "lebanon",
        "MY-SG-BN": "malaysia-singapore-brunei",  # Geofk shortcurt
        # "MV": "maldives",  # Island
        "MN": "mongolia",
        "MM": "myanmar",
        "NP": "nepal",
        "PK": "pakistan",
        "PH": "philippines",
        "LK": "sri-lanka",
        "SY": "syria",
        "TW": "taiwan",
        "TJ": "tajikistan",
        "TH": "thailand",
        "TM": "turkmenistan",
        "UZ": "uzbekistan",
        "VN": "vietnam",
        "YE": "yemen",
    },
    "australia-oceania": {
        # "AS": "american-oceania",  # Islands
        "AU": "australia",
        # "CK": "cook islands",  # Island
        # "FJ": "fiji",  # Islands
        # "PF": "french-polynesia",  # Islands
        # "GU": "guam",  # Island
        # "KI": "kiribati",  # Islands
        # "MH": "marshall islands",  # Islands
        # "FM": "micronesia",  # Islands
        # "NR": "nauru",  # Islands
        "NC": "new-caledonia",
        "NZ": "new-zealand",
        # "NU": "niue",  # Island
        # "NF": "norfolk island",  # Island
        # "MP": "northern mariana islands",  # Islands
        # "PW": "palau",  # Islands
        "PG": "papua-new-guinea",
        # "WS": "samoa",  # Islands
        # "SB": "solomon islands",  # Islands
        # "TK": "tokelau",  # Islands
        # "TO": "tonga",  # Islands
        # "TV": "tuvalu",  # Islands
        # "VU": "vanuatu",  # Islands
        # "WF": "wallis-et-futuna",  # Islands
    },
    "europe": {
        "AL": "albania",
        "AD": "andorra",
        "AT": "austria",
        "BY": "belarus",
        "BE": "belgium",
        "BA": "bosnia-herzegovina",
        "BG": "bulgaria",
        "HR": "croatia",
        "CZ": "czech-republic",
        "CY": "cyprus",
        "DK": "denmark",
        "EE": "estonia",
        # "FO": "faroe islands", # Islands
        "FI": "finland",
        "FR": "france",
        "GE": "georgia",
        "DE": "germany",
        # "GI": "gibraltar", # Peninsula; Isolated PS?
        "GR": "greece",
        # "GG": "guernsey", # Island
        "HU": "hungary",
        "IS": "iceland",
        "IE": "ireland-and-northern-ireland",
        # "IM": "isle of man", # Island
        "IT": "italy",
        # "JE": "jersey", # Island
        "LV": "latvia",
        "LI": "liechtenstein",
        "LT": "lithuania",
        "LU": "luxembourg",
        "MK": "macedonia",
        "MT": "malta",
        "MD": "moldova",
        "MC": "monaco",
        "ME": "montenegro",
        "NL": "netherlands",
        "NO": "norway",
        "PL": "poland",
        "PT": "portugal",
        "RO": "romania",
        "SM": "san-marino",
        "RS": "serbia",
        "SK": "slovakia",
        "SI": "slovenia",
        "ES": "spain",
        # "SJ": "svalbard-and-jan-mayen", # Islands
        "SE": "sweden",
        "CH": "switzerland",
        "UA": "ukraine",
        "GB": "great-britain",
        "TR": "turkey",
    },
    "russia": {
        "CEFD": "central-fed-district",
        "FEFD": "far-eastern-fed-district",
        "NCDF": "north-caucasus-fed-district",
        "NWDF": "northwestern-fed-district",
        "SBFD": "siberian-fed-district",
        "SOFD": "south-fed-district",
        "URDF": "ural-fed-district",
        "VOFD": "volga-fed-district",
        "RU": "russia",
    },
    "north-america": {
        "CA": "canada",
        "GL": "greenland",
        "MX": "mexico",
        "US": "us",
    },
    "south-america": {
        "AR": "argentina",
        "BO": "bolivia",
        "BR": "brazil",
        "CL": "chile",
        "CO": "colombia",
        "EC": "ecuador",
        "PE": "peru",
        "SR": "suriname",
        "PY": "paraguay",
        "UY": "uruguay",
        "VE": "venezuela",
    },
    "central-america": {
        "BZ": "belize",
        "GT": "guatemala",
        "SV": "el-salvador",
        "HN": "honduras",
        "NI": "nicaragua",
        "CR": "costa-rica",
    },
}

world_cc = {
    country_2D: country_name for d in world_geofk.values() for (country_2D, country_name) in d.items()
}

def get_continent_country(code):
    for continent in world_geofk:
        country = world_geofk[continent].get(code, 0)
        if country:
            return continent, country
    return continent, country