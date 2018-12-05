# Function to load data and set time -----------------------
# Use function toLocalTime
# Mark off-exchange negotiation with qualifier 221[IRGCOND] and 230[IRGCOND]

require(ff) # Memory-efficient storage of large data
require(data.table)
require(stringi) # Character string processing
require(lubridate) # For date-time parsing function

source("changeDepth.R") # Function for depth change

# Function to convert UTC offset time to local time----
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

# Function to load data----
loadTrades <- function(fileDir){
  # Load data with fread, requiring to add 7 Zip to the system path
  comTrade <- fread(input = paste0("7z x -so ", fileDir), header = TRUE, check.names=T)
  
  # Split Date and Time from local exchange time, then create the variable Second
  comTrade <- comTrade[, Date := toLocalTime(Date.Time)$Date]
  comTrade <- comTrade[, Time := toLocalTime(Date.Time)$Time]
  comTrade <- comTrade[order(comTrade$Date,comTrade$Time)]
  
  comTrade$Second <- strptime(comTrade$Time, format = "%H:%M:%OS", tz = "")$hour*3600+
    strptime(comTrade$Time, format = "%H:%M:%OS", tz = "")$min*60+
    strptime(comTrade$Time, format = "%H:%M:%OS", tz = "")$sec
  
  # Create name to 10-level depth
  L10Depths <- c("L1.BidPrice", "L1.BidSize", "L1.AskPrice", "L1.AskSize",
                 "L2.BidPrice", "L2.BidSize", "L2.AskPrice", "L2.AskSize",
                 "L3.BidPrice", "L3.BidSize", "L3.AskPrice", "L3.AskSize",
                 "L4.BidPrice", "L4.BidSize", "L4.AskPrice", "L4.AskSize",
                 "L5.BidPrice", "L5.BidSize", "L5.AskPrice", "L5.AskSize",
                 "L6.BidPrice", "L6.BidSize", "L6.AskPrice", "L6.AskSize",
                 "L7.BidPrice", "L7.BidSize", "L7.AskPrice", "L7.AskSize",
                 "L8.BidPrice", "L8.BidSize", "L8.AskPrice", "L8.AskSize",
                 "L9.BidPrice", "L9.BidSize", "L9.AskPrice", "L9.AskSize",
                 "L10.BidPrice", "L10.BidSize", "L10.AskPrice", "L10.AskSize")
  
  # Drop redundant variables for TAQ data
  if(grepl("TAQ", fileDir)|grepl("Trades", fileDir)) {
    # Mark delayed delivery
    comTrade <- comTrade[grepl("221\\[IRGCOND\\]", Qualifiers) | grepl("230\\[IRGCOND\\]", Qualifiers), Type := "offExchange"]
    
    # Drop other vairables
    comTrade <- comTrade[, .(X.RIC, Type, Price, Volume, Date, Time, Second)] # Off-exchange negociation mark is kept in the variable Type
    comTrade <- comTrade[Price!=0 & !is.na(Price),]
    
    # Special treatment to half-day trading
    halfTradeDay <- read.table("Half-trading days.txt", header = TRUE)
    halfTradeDay$Date <- as.character(halfTradeDay$Date)
    
    # Drop trades outside pre-trading, continuous trading, and post-trading sessions
    comTrade <- comTrade[comTrade$Type=="Trade" | comTrade$Type=="offExchange",] # Keep both standard and delayed delivery trades
    
    comTrade <- comTrade[(!comTrade$Date %in% halfTradeDay$Date & ((comTrade$Second >= 8*3600+30*60 & comTrade$Second < 8*3600+55*60) # Pre-trading
                                                                   | (comTrade$Second >= 9*3600 & comTrade$Second < 17*3600+25*60) # Continuous trading
                                                                   | (comTrade$Second >= 17*3600+30*60 & comTrade$Second < 18*3600))) # Post-trading
                         | (comTrade$Date %in% halfTradeDay$Date & (comTrade$Second >= 8*3600+30*60 & comTrade$Second < 8*3600+55*60) # Pre-trading
                            | (comTrade$Second >= 9*3600 & comTrade$Second < 12*3600+55*60) # Continuous trading
                            | (comTrade$Second >= 13*3600 & comTrade$Second < 13*3600+30*60)), # Post-trading
                         ]
    
    # Adding columns for depth data
    # comTrade[,(L10Depths)] <- NULL 
    comTrade[,c("bestBid", "bestAsk", "chgBid","chgAsk")] <- as.numeric(NA)
    comTrade[,(L10Depths)] <- as.numeric(NA)
  }
  
  # Drop redundant variables for Depth data
  if(grepl("Depth", fileDir)) {
    comTrade <- comTrade[, 
                         .(X.RIC, Type, Date, Time, Second,
                           L1.BidPrice, L1.BidSize, L1.AskPrice, L1.AskSize,
                           L2.BidPrice, L2.BidSize, L2.AskPrice, L2.AskSize,
                           L3.BidPrice, L3.BidSize, L3.AskPrice, L3.AskSize,
                           L4.BidPrice, L4.BidSize, L4.AskPrice, L4.AskSize,
                           L5.BidPrice, L5.BidSize, L5.AskPrice, L5.AskSize,
                           L6.BidPrice, L6.BidSize, L6.AskPrice, L6.AskSize,
                           L7.BidPrice, L7.BidSize, L7.AskPrice, L7.AskSize,
                           L8.BidPrice, L8.BidSize, L8.AskPrice, L8.AskSize,
                           L9.BidPrice, L9.BidSize, L9.AskPrice, L9.AskSize,
                           L10.BidPrice, L10.BidSize, L10.AskPrice, L10.AskSize)]
    
    comTrade <- comTrade[! apply( comTrade[, ..L10Depths] , 1 , function(x) all(is.na(x)) ) , ]
    
    # Chg. of depth as approx for trading volume
    # comTrade <- comTrade[, chgBid :=-diff(L1.BidSize, lag = 1, differences = 1), by=.(X.RIC, Date)]
    # comTrade <- comTrade[, chgAsk :=-diff(L1.AskSize, lag = 1, differences = 1), by=.(X.RIC, Date)]
    # New approach to calculate depth size change
    comTrade[, c("chgBid", "chgAsk"):= chgDepth(comTrade), by=.(X.RIC, Date)]
  }
  
  comTrade[, Date:=as.character(Date)]
  return(comTrade)
}