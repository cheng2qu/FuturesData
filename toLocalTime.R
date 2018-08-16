# Identify time zone with UTC offset character
# Adjust different time to local time
require(stringi)
require(lubridate)

toLocalTime <- function(Date.Time){
  # Input Date.Time format as "yyyy-mm-ddThh:mm:OSz"
  op <- options(digits.secs=6) # Set format of fractional second
  localDateTime <- with_tz(parse_date_time2(Date.Time, "YmdHMOSz"), tzone = "")
  Date <- substr(localDateTime,1,10)
  Time <- substr(localDateTime,12,27)
  return(data.frame("Date" = Date, "Time" = Time))
}

# Example
# x <- c("2014-11-07T14:12:03.9917315143+01","2014-11-07T14:12:03.9917315143Z")
# toLocalTime(x)$Date
# toLocalTime(x)$Time