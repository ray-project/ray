const getCurrentUTCOffset = (timeZone: string) => {
  try {
    const date = new Date();
    const formatter = new Intl.DateTimeFormat("en-US", {
      timeZone,
      timeZoneName: "shortOffset",
    });
    // Extract the GMT offset part from the formatted string
    const formatted = formatter.format(date);
    const match = formatted.match(/GMT([+-]\d{1,2}(?::\d{2})?)?/);
    return match ? match[0] : "";
  } catch (e) {
    console.warn(`Error getting UTC offset for ${timeZone}:`, e);
    return "";
  }
};

// Base timezone definitions
const baseTimezones = [
  {
    value: "Etc/GMT+12",
    group: "Pacific",
    country: "International Date Line West",
  },
  {
    value: "Pacific/Pago_Pago",
    group: "Pacific",
    country: "American Samoa",
  },
  {
    value: "Pacific/Midway",
    group: "Pacific",
    country: "Midway Island",
  },
  {
    value: "Pacific/Honolulu",
    group: "Pacific",
    country: "Hawaii",
  },
  {
    value: "America/Anchorage",
    group: "America",
    country: "Alaska",
  },
  {
    value: "America/Los_Angeles",
    group: "America",
    country: "Pacific Time (US & Canada)",
  },
  {
    value: "America/Tijuana",
    group: "America",
    country: "Tijuana",
  },
  {
    value: "America/Phoenix",
    group: "America",
    country: "Arizona",
  },
  {
    value: "America/Mazatlan",
    group: "America",
    country: "Mazatlan",
  },
  {
    value: "America/Denver",
    group: "America",
    country: "Mountain Time (US & Canada)",
  },
  {
    value: "America/Guatemala",
    group: "America",
    country: "Central America",
  },
  {
    value: "America/Chicago",
    group: "America",
    country: "Central Time (US & Canada)",
  },
  {
    value: "America/Chihuahua",
    group: "America",
    country: "Chihuahua",
  },
  {
    value: "America/Mexico_City",
    group: "America",
    country: "Mexico City",
  },
  {
    value: "America/Monterrey",
    group: "America",
    country: "Monterrey",
  },
  {
    value: "America/Regina",
    group: "America",
    country: "Saskatchewan",
  },
  {
    value: "America/Bogota",
    group: "America",
    country: "Bogota",
  },
  {
    value: "America/New_York",
    group: "America",
    country: "Eastern Time (US & Canada)",
  },
  {
    value: "America/Indiana/Indianapolis",
    group: "America",
    country: "Indiana (East)",
  },
  {
    value: "America/Lima",
    group: "America",
    country: "Lima",
  },
  {
    value: "America/Guayaquil",
    group: "America",
    country: "Quito",
  },
  {
    value: "America/Halifax",
    group: "America",
    country: "Atlantic Time (Canada)",
  },
  {
    value: "America/Caracas",
    group: "America",
    country: "Caracas",
  },
  {
    value: "America/Guyana",
    group: "America",
    country: "Georgetown",
  },
  {
    value: "America/La_Paz",
    group: "America",
    country: "La Paz",
  },
  {
    value: "America/Puerto_Rico",
    group: "America",
    country: "Puerto Rico",
  },
  {
    value: "America/Santiago",
    group: "America",
    country: "Santiago",
  },
  {
    value: "America/St_Johns",
    group: "America",
    country: "Newfoundland",
  },
  {
    value: "America/Sao_Paulo",
    group: "America",
    country: "Brasilia",
  },
  {
    value: "America/Argentina/Buenos_Aires",
    group: "America",
    country: "Buenos Aires",
  },
  {
    value: "America/Montevideo",
    group: "America",
    country: "Montevideo",
  },
  {
    value: "America/Godthab",
    group: "America",
    country: "Greenland",
  },
  {
    value: "Etc/GMT+2",
    group: "Atlantic",
    country: "Mid-Atlantic",
  },
  {
    value: "Atlantic/Azores",
    group: "Atlantic",
    country: "Azores",
  },
  {
    value: "Atlantic/Cape_Verde",
    group: "Atlantic",
    country: "Cape Verde Is.",
  },
  {
    value: "Europe/London",
    group: "Europe",
    country: "Edinburgh",
  },
  {
    value: "Europe/Lisbon",
    group: "Europe",
    country: "Lisbon",
  },
  {
    value: "Europe/London",
    group: "Europe",
    country: "London",
  },
  {
    value: "Africa/Monrovia",
    group: "Africa",
    country: "Monrovia",
  },
  {
    value: "Europe/Amsterdam",
    group: "Europe",
    country: "Amsterdam",
  },
  {
    value: "Europe/Belgrade",
    group: "Europe",
    country: "Belgrade",
  },
  {
    value: "Europe/Berlin",
    group: "Europe",
    country: "Berlin",
  },
  {
    value: "Europe/Brussels",
    group: "Europe",
    country: "Brussels",
  },
  {
    value: "Europe/Budapest",
    group: "Europe",
    country: "Budapest",
  },
  {
    value: "Europe/Copenhagen",
    group: "Europe",
    country: "Copenhagen",
  },
  {
    value: "Europe/Madrid",
    group: "Europe",
    country: "Madrid",
  },
  {
    value: "Europe/Paris",
    group: "Europe",
    country: "Paris",
  },
  {
    value: "Europe/Prague",
    group: "Europe",
    country: "Prague",
  },
  {
    value: "Europe/Rome",
    group: "Europe",
    country: "Rome",
  },
  {
    value: "Europe/Sarajevo",
    group: "Europe",
    country: "Sarajevo",
  },
  {
    value: "Europe/Stockholm",
    group: "Europe",
    country: "Stockholm",
  },
  {
    value: "Europe/Vienna",
    group: "Europe",
    country: "Vienna",
  },
  {
    value: "Europe/Warsaw",
    group: "Europe",
    country: "Warsaw",
  },
  {
    value: "Africa/Lagos",
    group: "Africa",
    country: "West Central Africa",
  },
  {
    value: "Asia/Amman",
    group: "Asia",
    country: "Amman",
  },
  {
    value: "Europe/Athens",
    group: "Europe",
    country: "Athens",
  },
  {
    value: "Asia/Beirut",
    group: "Asia",
    country: "Beirut",
  },
  {
    value: "Europe/Bucharest",
    group: "Europe",
    country: "Bucharest",
  },
  {
    value: "Africa/Cairo",
    group: "Africa",
    country: "Cairo",
  },
  {
    value: "Africa/Harare",
    group: "Africa",
    country: "Harare",
  },
  {
    value: "Europe/Helsinki",
    group: "Europe",
    country: "Helsinki",
  },
  {
    value: "Europe/Istanbul",
    group: "Europe",
    country: "Istanbul",
  },
  {
    value: "Asia/Jerusalem",
    group: "Asia",
    country: "Jerusalem",
  },
  {
    value: "Europe/Kiev",
    group: "Europe",
    country: "Kyiv",
  },
  {
    value: "Europe/Minsk",
    group: "Europe",
    country: "Minsk",
  },
  {
    value: "Europe/Riga",
    group: "Europe",
    country: "Riga",
  },
  {
    value: "Europe/Sofia",
    group: "Europe",
    country: "Sofia",
  },
  {
    value: "Europe/Tallinn",
    group: "Europe",
    country: "Tallinn",
  },
  {
    value: "Europe/Vilnius",
    group: "Europe",
    country: "Vilnius",
  },
  {
    value: "Asia/Baghdad",
    group: "Asia",
    country: "Baghdad",
  },
  {
    value: "Asia/Kuwait",
    group: "Asia",
    country: "Kuwait",
  },
  {
    value: "Europe/Moscow",
    group: "Europe",
    country: "Moscow",
  },
  {
    value: "Africa/Nairobi",
    group: "Africa",
    country: "Nairobi",
  },
  {
    value: "Asia/Riyadh",
    group: "Asia",
    country: "Riyadh",
  },
  {
    value: "Asia/Tehran",
    group: "Asia",
    country: "Tehran",
  },
  {
    value: "Asia/Dubai",
    group: "Asia",
    country: "Abu Dhabi",
  },
  {
    value: "Asia/Baku",
    group: "Asia",
    country: "Baku",
  },
  {
    value: "Asia/Muscat",
    group: "Asia",
    country: "Muscat",
  },
  {
    value: "Asia/Tbilisi",
    group: "Asia",
    country: "Tbilisi",
  },
  {
    value: "Asia/Yerevan",
    group: "Asia",
    country: "Yerevan",
  },
  {
    value: "Asia/Kabul",
    group: "Asia",
    country: "Kabul",
  },
  {
    value: "Asia/Karachi",
    group: "Asia",
    country: "Islamabad",
  },
  {
    value: "Asia/Tashkent",
    group: "Asia",
    country: "Tashkent",
  },
  {
    value: "Asia/Kolkata",
    group: "Asia",
    country: "Chennai",
  },
  {
    value: "Asia/Kolkata",
    group: "Asia",
    country: "Kolkata",
  },
  {
    value: "Asia/Kolkata",
    group: "Asia",
    country: "Mumbai",
  },
  {
    value: "Asia/Kolkata",
    group: "Asia",
    country: "New Delhi",
  },
  {
    value: "Asia/Kathmandu",
    group: "Asia",
    country: "Kathmandu",
  },
  {
    value: "Asia/Almaty",
    group: "Asia",
    country: "Almaty",
  },
  {
    value: "Asia/Dhaka",
    group: "Asia",
    country: "Dhaka",
  },
  {
    value: "Asia/Yekaterinburg",
    group: "Asia",
    country: "Yekaterinburg",
  },
  {
    value: "Asia/Yangon",
    group: "Asia",
    country: "Yangon (Rangoon)",
  },
  {
    value: "Asia/Bangkok",
    group: "Asia",
    country: "Bangkok",
  },
  {
    value: "Asia/Jakarta",
    group: "Asia",
    country: "Jakarta",
  },
  {
    value: "Asia/Novosibirsk",
    group: "Asia",
    country: "Novosibirsk",
  },
  {
    value: "Asia/Shanghai",
    group: "Asia",
    country: "Beijing",
  },
  {
    value: "Asia/Chongqing",
    group: "Asia",
    country: "Chongqing",
  },
  {
    value: "Asia/Hong_Kong",
    group: "Asia",
    country: "Hong Kong",
  },
  {
    value: "Asia/Krasnoyarsk",
    group: "Asia",
    country: "Krasnoyarsk",
  },
  {
    value: "Asia/Kuala_Lumpur",
    group: "Asia",
    country: "Kuala Lumpur",
  },
  {
    value: "Australia/Perth",
    group: "Australia",
    country: "Perth",
  },
  {
    value: "Asia/Singapore",
    group: "Asia",
    country: "Singapore",
  },
  {
    value: "Asia/Taipei",
    group: "Asia",
    country: "Taipei",
  },
  {
    value: "Asia/Ulaanbaatar",
    group: "Asia",
    country: "Ulaan Bataar",
  },
  {
    value: "Asia/Urumqi",
    group: "Asia",
    country: "Urumqi",
  },
  {
    value: "Asia/Irkutsk",
    group: "Asia",
    country: "Irkutsk",
  },
  {
    value: "Asia/Tokyo",
    group: "Asia",
    country: "Osaka",
  },
  {
    value: "Asia/Tokyo",
    group: "Asia",
    country: "Sapporo",
  },
  {
    value: "Asia/Seoul",
    group: "Asia",
    country: "Seoul",
  },
  {
    value: "Asia/Tokyo",
    group: "Asia",
    country: "Tokyo",
  },
  {
    value: "Australia/Adelaide",
    group: "Australia",
    country: "Adelaide",
  },
  {
    value: "Australia/Darwin",
    group: "Australia",
    country: "Darwin",
  },
  {
    value: "Australia/Brisbane",
    group: "Australia",
    country: "Brisbane",
  },
  {
    value: "Australia/Sydney",
    group: "Australia",
    country: "Canberra",
  },
  {
    value: "Pacific/Guam",
    group: "Pacific",
    country: "Guam",
  },
  {
    value: "Australia/Hobart",
    group: "Australia",
    country: "Hobart",
  },
  {
    value: "Australia/Melbourne",
    group: "Australia",
    country: "Melbourne",
  },
  {
    value: "Pacific/Port_Moresby",
    group: "Pacific",
    country: "Port Moresby",
  },
  {
    value: "Australia/Sydney",
    group: "Australia",
    country: "Sydney",
  },
  {
    value: "Asia/Magadan",
    group: "Asia",
    country: "Magadan",
  },
  {
    value: "Pacific/Noumea",
    group: "Pacific",
    country: "New Caledonia",
  },
  {
    value: "Pacific/Guadalcanal",
    group: "Pacific",
    country: "Solomon Is.",
  },
  {
    value: "Pacific/Auckland",
    group: "Pacific",
    country: "Auckland",
  },
  {
    value: "Pacific/Fiji",
    group: "Pacific",
    country: "Fiji",
  },
  {
    value: "Asia/Kamchatka",
    group: "Asia",
    country: "Kamchatka",
  },
  {
    value: "Pacific/Majuro",
    group: "Pacific",
    country: "Marshall Is.",
  },
  {
    value: "Pacific/Auckland",
    group: "Pacific",
    country: "Wellington",
  },
  {
    value: "Pacific/Tongatapu",
    group: "Pacific",
    country: "Nuku'alofa",
  },
  {
    value: "Pacific/Apia",
    group: "Pacific",
    country: "Samoa",
  },
  {
    value: "Pacific/Fakaofo",
    group: "Pacific",
    country: "Tokelau Is.",
  },
];

// Generate the timezone list with current UTC offsets
export const timezones = baseTimezones.map((tz) => ({
  ...tz,
  utc: getCurrentUTCOffset(tz.value),
}));
