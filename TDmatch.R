require(data.table)
require(ff)
require(stringi)
require(lubridate)


# Function to compress gz file
source("gzcp-GZCompress.R")

# setwd("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/")

# Function searching for closest matched depth -----------------------
closestDepth <- function(tradeSecond, depthSecond, tradeDate) {
  
  # Half trading day according to the trading calendar
  halfTradeDay <- read.table("Half-trading days.txt", header = TRUE)
  halfTradeDay$Date <- as.character(halfTradeDay$Date)
  
  if (tradeDate %in% halfTradeDay$Date & tradeSecond>=min(depthSecond)) {    
    ind <- which.max((depthSecond- tradeSecond)[(depthSecond- tradeSecond)<=0]) # Find the index of the closest quote before
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 12*3600+55*60, 13*3600, 13*3600+30*60)
    
    if (findInterval(tradeSecond, hourInterval)!=
        findInterval(depthSecond[which.max((depthSecond- tradeSecond)[(depthSecond- tradeSecond)<=0])], hourInterval)) {
      ind <- NA
    }
  } else {
    if (tradeSecond>=min(depthSecond)) {
      ind <- which.max((depthSecond- tradeSecond)[(depthSecond- tradeSecond)<=0])
      hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 17*3600+25*60, 17*3600+30*60, 18*3600)
      
      if (findInterval(tradeSecond, hourInterval)!=
          findInterval(depthSecond[which.max((depthSecond- tradeSecond)[(depthSecond- tradeSecond)<=0])], hourInterval)) {
        ind <- NA
      }
    } else {
      ind <- which.min(depthSecond)
    }
  }
  if (length(ind)>1) ind <- ind[1]
  return(ind)
}

# Function searching for closest matched trade -----------------------
closestTrade <- function(tradeSecond, trade2Second, tradeDate) {
  # Half trading day according to the trading calendar
  halfTradeDay <- read.table("Half-trading days.txt", header = TRUE)
  halfTradeDay$Date <- as.character(halfTradeDay$Date)
  
  if (tradeDate %in% halfTradeDay$Date & tradeSecond>=min(trade2Second)) {    
    ind <- which.min(abs(trade2Second- tradeSecond)) # Find the index of the closest quote around: before or after
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 12*3600+55*60, 13*3600, 13*3600+30*60)
    
    if (findInterval(tradeSecond, hourInterval)!=
        findInterval(trade2Second[which.min(abs(trade2Second- tradeSecond))], hourInterval)) {
      ind <- NA
    }
  } else {
    if (tradeSecond>=min(trade2Second)) {
      ind <- which.min(abs(trade2Second- tradeSecond))
      hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 17*3600+25*60, 17*3600+30*60, 18*3600)
      
      if (findInterval(tradeSecond, hourInterval)!=
          findInterval(trade2Second[which.min(abs(trade2Second- tradeSecond))], hourInterval)) {
        ind <- NA
      }
    } else {
      ind <- which.min(trade2Second)
    }
  }
  if (length(ind)>1) ind <- ind[1]
  return(ind)
}

# Function to get nth unique minimum -----------------------
min.n <- function(x,n) {
  x <- unique(x)
  if (length(x)<n) {
    return(tail(x,1))
  } else {
    return(sort(x, decreasing = FALSE)[n])
  }
}

# Function to load data and set time -----------------------
# Function to load data
loadTrades <- function(fileDir){
  # Load data with fread, requiring to add 7 Zip to the system path
  comTrade <- fread(input = paste0("7z x -so ", fileDir), header = TRUE, check.names=T)
  
  # Split Date and Time, then create the variable Second
  comTrade <- comTrade[, Date := substr(Date.Time,1,10)]
  comTrade <- comTrade[, Time := sub("T", "", sub("^[^T]*", "", Date.Time))]
  
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
  if(grepl("TAQ", fileDir)) {
    # Delete delayed delivery
    comTrade <- comTrade[!grepl("221\\[IRGCOND\\]", Qualifiers) & !grepl("230\\[IRGCOND\\]", Qualifiers), .(X.RIC, Type, Price, Volume, Date, Time, Second)]
    comTrade <- comTrade[Price!=0 & !is.na(Price),]
    
    # Special treatment to half-day trading
    halfTradeDay <- read.table("Z:/Documents/Futures rollover/Half-trading days.txt", header = TRUE)
    halfTradeDay$Date <- as.character(halfTradeDay$Date)
    
    # Drop trades outside pre-trading, continuous trading, and post-trading sessions
    comTrade <- comTrade[comTrade$Type=="Trade",]
    
    comTrade <- comTrade[(!comTrade$Date %in% halfTradeDay$Date & ((comTrade$Second >= 8*3600+30*60 & comTrade$Second < 8*3600+55*60) # Pre-trading
                                                                   | (comTrade$Second >= 9*3600 & comTrade$Second < 17*3600+25*60) # Continuous trading
                                                                   | (comTrade$Second >= 17*3600+30*60 & comTrade$Second < 18*3600))) # Post-trading
                         | (comTrade$Date %in% halfTradeDay$Date & (comTrade$Second >= 8*3600+30*60 & comTrade$Second < 8*3600+55*60) # Pre-trading
                            | (comTrade$Second >= 9*3600 & comTrade$Second < 12*3600+55*60) # Continuous trading
                            | (comTrade$Second >= 13*3600 & comTrade$Second < 13*3600+30*60)), # Post-trading
                         ]
    
    # Adding columns for depth data
    # comTrade[,(L10Depths)] <- NULL 
    comTrade[,c("bestBid", "bestAsk", "chgBid","chgAsk")] <- numeric(0)
    comTrade[,(L10Depths)] <- numeric(0)
  }
  
  # Drop redundant variables for Depth data
  if(grepl("Depth", fileDir)) {
    comTrade <- comTrade[which(!is.na(comTrade[, ..L10Depths])), 
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
    
    # Chg. of depth as approx for trading volume
    comTrade <- comTrade[, chgBid :=-diff(L1.BidSize, lag = 1, differences = 1), by=.(X.RIC, Date)]
    comTrade <- comTrade[, chgAsk :=-diff(L1.AskSize, lag = 1, differences = 1), by=.(X.RIC, Date)]
  }
  return(comTrade)
}

# function to fix trading session----
ifDiffSession <- function(tradeDate,tradeSec,depthSec){
  # Half trading day according to the trading calendar
  halfTradeDay <- read.table("Z:/Documents/Futures rollover/Half-trading days.txt", header = TRUE)
  halfTradeDay$Date <- as.character(halfTradeDay$Date)
  if (tradeDate %in% halfTradeDay$Date) {  
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 12*3600+55*60, 13*3600, 13*3600+30*60)
    return(findInterval(tradeSec, hourInterval)!= findInterval(depthSec, hourInterval))
  } else {
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 17*3600+25*60, 17*3600+30*60, 18*3600)
    return(findInterval(tradeSec, hourInterval)!= findInterval(depthSec, hourInterval))
  }
}

# Function to match reg depth with reg trades -----------------------
# longTrade <- loadTrades("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/OMXS30V7_2017-10-02.csv.gz")
# longDepthFile <- "E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth/OMXS30V7_2017-10-02.csv.gz"
tdRegMatch <- function(longTrade, folderDepth) {
  # The function is modified from tdComMatch
  # So the data is named with prefix com-
  comTrade <- longTrade
  comDepth <- loadTrades(fileDir = paste0(folderDepth,comTrade$X.RIC[1],"_",comTrade$Date[1],".csv.gz"))
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
  # Trade is the data for trades
  # Depth is the data for 5-level depths  
  if (nrow(comTrade)==0) return(paste0("No trades for ", comTrade$X.RIC[1]," on ", comTrade$Date[1]))
  if (nrow(comDepth)==0) return(paste0("No trades for ", comDepth$X.RIC[1]," on ", comDepth$Date[1]))
  # print(paste(comTrade$X.RIC[1], comTrade$Date[1], length(which(is.na(comTrade$L1.BidPrice)))))
  
  # Matching with interval between chg. of depth where the trade happens
  for (i in 1:nrow(comTrade)) { #nrow(comTrade)
    # Skip matched lines
    if (!is.na(comTrade$L1.BidPrice[i])) next
    
    if(i==1){
      # If start from pre-trade or normal session
      tradeStart <- c(8*3600+30*60, 8*3600+55*60)[1+(comTrade$Second[i]>=8*3600+55*60)]
      
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second <= comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])>=(tradeStart - comTrade$Second[i])&
                            ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])|
                               (comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$L1.BidSize==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$L1.AskSize==comTrade$Volume[i])))
      if(length(indexDepth)==0){
        indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                              comDepth$Date == comTrade$Date[i]&
                              comDepth$Second <= comTrade$Second[i]&
                              (comDepth$Second - comTrade$Second[i])>=(tradeStart - comTrade$Second[i])&
                              comDepth$L1.BidPrice<=comTrade$Price[i] & comDepth$L1.AskPrice>=comTrade$Price[i])
      }
      if (length(indexDepth)>=1) {
        indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
        comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      }
      next
    }
    
    # Exclude matching cross trading session
    indexDepth <- which.max((comDepth$Second-comTrade$Second[i])[(comDepth$Second-comTrade$Second[i])<=0])
    if(ifDiffSession(comTrade[i,Date],comTrade[i,Second],comDepth[indexDepth,Second])) {
      comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- NA
      comTrade[i][,(L10Depths)] <- NA
      next
    }
    
    # Find the index of depth change by same date, same time interval between chg. of depth, price, and vol.
    # 1.Exactly matched====
    indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                          comDepth$Date == comTrade$Date[i]&
                          comDepth$Second <= comTrade$Second[i]&
                          (comDepth$Second - comTrade$Second[i])>=(comTrade$Second[i-1] - comTrade$Second[i])&
                          ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                             (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])|
                             (comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$L1.BidSize==comTrade$Volume[i]) |
                             (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$L1.AskSize==comTrade$Volume[i])))
    
    # 2. Match to next depth if no trades happen in the interval====
    if (length(indexDepth)==0) {
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second > comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])<(comTrade$Second[i+1] - comDepth$Second)&
                            ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])|
                               (comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$L1.BidSize==comTrade$Volume[i])|
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$L1.AskSize==comTrade$Volume[i])))
      
      indexDepthNext <- which(comDepth$X.RIC == comTrade$X.RIC[i+1]& 
                                comDepth$Date == comTrade$Date[i+1]&
                                comDepth$Second <= comTrade$Second[i+1]&
                                (comDepth$Second - comTrade$Second[i+1])>=(comTrade$Second[i] - comTrade$Second[i+1])&
                                ((comDepth$L1.BidPrice==comTrade$Price[i+1] & comDepth$chgBid==comTrade$Volume[i+1]) |
                                   (comDepth$L1.AskPrice==comTrade$Price[i+1] & comDepth$chgAsk==comTrade$Volume[i+1])|
                                   (comDepth$L1.BidPrice==comTrade$Price[i+1] & comDepth$L1.BidSize==comTrade$Volume[i+1]) |
                                   (comDepth$L1.AskPrice==comTrade$Price[i+1] & comDepth$L1.AskSize==comTrade$Volume[i+1])))
      indexDepth <- indexDepth[!indexDepth %in% indexDepthNext]
    }
    
    # 3.No match at quotes, try quote bounds====
    if (length(indexDepth)==0) {
      # If there is no fully match, try to find match of split trades with same date, same time interval between chg. of depth, and price bounds
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second <= comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])>=(comTrade$Second[i-1] - comTrade$Second[i])&
                            comDepth$L1.BidPrice<=comTrade$Price[i] & comDepth$L1.AskPrice>=comTrade$Price[i] &
                            (comDepth$chgBid==comTrade$Volume[i] | comDepth$chgAsk==comTrade$Volume[i] |
                               comDepth$L1.BidSize==comTrade$Volume[i] | comDepth$L1.AskSize==comTrade$Volume[i]))  
    }
    
    # 4. Only consider quote bounds and time====
    if (length(indexDepth)==0) {
      # If there is no fully match, try to find match of split trades with same date, same time interval, and price bounds
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second <= comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])>=(comTrade$Second[i-1] - comTrade$Second[i])&
                            (comDepth$L1.BidPrice<=comTrade$Price[i] & comDepth$L1.AskPrice>=comTrade$Price[i]))  
    }
    
    # 5.No depth between two trades====
    if (length(indexDepth)==0) {
      if (length(which(comDepth$Second >= comTrade$Second[i-1] & comDepth$Second <= comTrade$Second[i]))==0) {
        if (comTrade$Price[i-1]==comTrade$Price[i]) {
          comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("bestBid", "bestAsk", "chgBid", "chgAsk")]
          comTrade[i][,(L10Depths)] <- comTrade[i-1,..L10Depths]
          next
        } else {
          # Consider same depth
          if(!is.na(comTrade[i-1, L1.BidPrice]) & !is.na(comTrade[i-1, L1.AskPrice])) {
            if(comTrade[i-1, L1.BidPrice]<=comTrade$Price[i] &  comTrade[i-1, L1.AskPrice]>=comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L1.AskPrice","L1.BidSize","chgAsk")]
              comTrade[i][,(L10Depths)] <- comTrade[i-1, ..L10Depths]
              next
            }
          }
          # If there is no depth between two trades, try to find match cross depths
          if(!is.na(comTrade[i-1, L2.BidPrice])) {
            if(comTrade[i-1, L2.BidPrice]==comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L2.BidPrice","L1.AskPrice","L2.BidSize","chgAsk")]
              comTrade[i][,(L10Depths)] <-  cbind(comTrade[i-1, c("L2.BidPrice", "L2.BidSize", "L1.AskPrice", "L1.AskSize",
                                                                  "L3.BidPrice", "L3.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                  "L4.BidPrice", "L4.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                  "L5.BidPrice", "L5.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                  "L6.BidPrice", "L6.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                  "L7.BidPrice", "L7.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                  "L8.BidPrice", "L8.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                  "L9.BidPrice", "L9.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                  "L10.BidPrice", "L10.BidSize", "L9.AskPrice", "L9.AskSize")],
                                                  "empty1"=NA, "empty2"=NA,
                                                  comTrade[i-1, c("L10.AskPrice", "L10.AskSize")])
              next
            }
          }
          if(!is.na(comTrade[i-1, L2.AskPrice])) {
            if(comTrade[i-1,L2.AskPrice]==comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
              comTrade[i][,(L10Depths)] <- cbind(comTrade[i-1, c("L1.BidPrice", "L1.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                 "L2.BidPrice", "L2.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                 "L3.BidPrice", "L3.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                 "L4.BidPrice", "L4.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                 "L5.BidPrice", "L5.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                 "L6.BidPrice", "L6.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                 "L7.BidPrice", "L7.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                 "L8.BidPrice", "L8.BidSize", "L9.AskPrice", "L9.AskSize",
                                                                 "L9.BidPrice", "L9.BidSize", "L10.AskPrice", "L10.AskSize",
                                                                 "L10.BidPrice", "L10.BidSize")],
                                                 "empty1"=NA, "empty2"=NA)
              next
            }
          }
          if(!is.na(comTrade[i-1, L2.BidPrice]) & !is.na(comTrade[i-1, L1.AskPrice])) {
            if(comTrade[i-1, L2.BidPrice]<=comTrade$Price[i] & comTrade[i-1, L1.AskPrice]>=comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L2.BidPrice","L1.AskPrice","L2.BidSize","chgAsk")]
              comTrade[i][,(L10Depths)] <-  cbind(comTrade[i-1, c("L2.BidPrice", "L2.BidSize", "L1.AskPrice", "L1.AskSize",
                                                                  "L3.BidPrice", "L3.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                  "L4.BidPrice", "L4.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                  "L5.BidPrice", "L5.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                  "L6.BidPrice", "L6.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                  "L7.BidPrice", "L7.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                  "L8.BidPrice", "L8.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                  "L9.BidPrice", "L9.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                  "L10.BidPrice", "L10.BidSize", "L9.AskPrice", "L9.AskSize")],
                                                  "empty1"=NA, "empty2"=NA,
                                                  comTrade[i-1, c("L10.AskPrice", "L10.AskSize")])
              next
            }
          }
          if(!is.na(comTrade[i-1, L1.BidPrice]) & !is.na(comTrade[i-1, L2.AskPrice])) {
            if(comTrade[i-1, L1.BidPrice]<=comTrade$Price[i] & comTrade[i-1,L2.AskPrice]>=comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
              comTrade[i][,(L10Depths)] <- cbind(comTrade[i-1, c("L1.BidPrice", "L1.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                 "L2.BidPrice", "L2.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                 "L3.BidPrice", "L3.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                 "L4.BidPrice", "L4.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                 "L5.BidPrice", "L5.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                 "L6.BidPrice", "L6.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                 "L7.BidPrice", "L7.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                 "L8.BidPrice", "L8.BidSize", "L9.AskPrice", "L9.AskSize",
                                                                 "L9.BidPrice", "L9.BidSize", "L10.AskPrice", "L10.AskSize",
                                                                 "L10.BidPrice", "L10.BidSize")],
                                                 "empty1"=NA, "empty2"=NA)
              next
            }
          } else{
            comTrade[i][,(L10Depths)] <- comTrade[i-1, ..L10Depths]
          }
        }
      } else{
        indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                              comDepth$Date == comTrade$Date[i]&
                              comDepth$Second <= comTrade$Second[i]&
                              (comDepth$Second - comTrade$Second[i])>=(comTrade$Second[i-1] - comTrade$Second[i]))
        # 6. Cross depth====
        if(length(indexDepth)>0){
          indexDepth <- max(indexDepth)
          if(!is.na(comDepth[indexDepth, L2.BidPrice]) & !is.na(comDepth[indexDepth, L1.AskPrice])){
            if(comDepth[indexDepth, L2.BidPrice]<=comTrade$Price[i] & comDepth[indexDepth, L1.AskPrice]>=comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L2.BidPrice","L1.AskPrice","L2.BidSize","chgAsk")]
            comTrade[i][,(L10Depths)] <-  cbind(comDepth[indexDepth, c("L2.BidPrice", "L2.BidSize", "L1.AskPrice", "L1.AskSize",
                                                                       "L3.BidPrice", "L3.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                       "L4.BidPrice", "L4.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                       "L5.BidPrice", "L5.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                       "L6.BidPrice", "L6.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                       "L7.BidPrice", "L7.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                       "L8.BidPrice", "L8.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                       "L9.BidPrice", "L9.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                       "L10.BidPrice", "L10.BidSize", "L9.AskPrice", "L9.AskSize")],
                                                "empty1"=NA, "empty2"=NA,
                                                comDepth[indexDepth, c("L10.AskPrice", "L10.AskSize")])
            next
            }
          }
          
          if(!is.na(comDepth[indexDepth, L1.BidPrice]) & !is.na(comDepth[indexDepth, L2.AskPrice])){
            if(comDepth[indexDepth, L1.BidPrice]<=comTrade$Price[i] & comDepth[indexDepth,L2.AskPrice]>=comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
              comTrade[i][,(L10Depths)] <- cbind(comDepth[indexDepth, c("L1.BidPrice", "L1.BidSize", "L2.AskPrice", "L2.AskSize",
                                                                        "L2.BidPrice", "L2.BidSize", "L3.AskPrice", "L3.AskSize",
                                                                        "L3.BidPrice", "L3.BidSize", "L4.AskPrice", "L4.AskSize",
                                                                        "L4.BidPrice", "L4.BidSize", "L5.AskPrice", "L5.AskSize",
                                                                        "L5.BidPrice", "L5.BidSize", "L6.AskPrice", "L6.AskSize",
                                                                        "L6.BidPrice", "L6.BidSize", "L7.AskPrice", "L7.AskSize",
                                                                        "L7.BidPrice", "L7.BidSize", "L8.AskPrice", "L8.AskSize",
                                                                        "L8.BidPrice", "L8.BidSize", "L9.AskPrice", "L9.AskSize",
                                                                        "L9.BidPrice", "L9.BidSize", "L10.AskPrice", "L10.AskSize",
                                                                        "L10.BidPrice", "L10.BidSize")],
                                                 "empty1"=NA, "empty2"=NA)
              next
            }
          }
        }
      }
    }
    
    if (length(indexDepth)>=1) {
      indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
      comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
      comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
    } else {
      # If there is any match, find the series trades potentially from the same chg. of depth and same price
      indexDepth <-  which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                             comDepth$Date == comTrade$Date[i]&
                             comDepth$Second <= comTrade$Second[i]&
                             (comDepth$Second - comTrade$Second[i])>=(comTrade$Second[i-1] - comTrade$Second[i]))
      if(length(indexDepth)>1) {
        indexDepth <- max(indexDepth)
      }
      indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                            comTrade$Date==comTrade$Date[i] &
                            comTrade$Second>=comTrade$Second[i] &
                            abs(comTrade$Second-comTrade$Second[i])<=0.01 &
                            abs(comTrade$Price-comTrade$Price[i])<=0.05)
      
      matchTrade <- which(comDepth[indexDepth, L1.BidPrice]<=comTrade$Price[indexTrade] & 
                            comDepth[indexDepth, L1.AskPrice]>=comTrade$Price[indexTrade])
      if (length(matchTrade)>0) {
        comTrade[indexTrade, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
        comTrade[indexTrade][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      } else{
        indexDepth <- which.max((comDepth$Second - comTrade$Second[i])[(comDepth$Second - comTrade$Second[i])<=0])
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      }
    }
    # indexDepthLast <- indexDepth # Keep last index
    indexDepth <- NULL
    # which(is.na(comTrade$L1.BidPrice))
    # unmatch <- comTrade[which(comTrade$Price<comTrade$L1.BidPrice|comTrade$Price>comTrade$L1.AskPrice|is.na(comTrade$L1.AskPrice)),]
    # length(which(is.na(comTrade$L1.BidPrice)))
    # length(which((comTrade$Price<comTrade$L1.BidPrice|comTrade$Price>comTrade$L1.AskPrice) & comTrade$Volume>=100))
  }
  
  comTrade <- comTrade[Price==bestBid, initiate := "Sell"][Price==bestAsk, initiate := "Buy"]
  return(comTrade)
}

# Function to move quotes when trading through-----
ttDepth <- function(side, depthBefore) {
  if (side=="B") {
    depthAfter <- cbind(depthBefore[,c("L2.BidPrice", "L2.BidSize", "L1.AskPrice", "L1.AskSize",
                                       "L3.BidPrice", "L3.BidSize", "L2.AskPrice", "L2.AskSize",
                                       "L4.BidPrice", "L4.BidSize", "L3.AskPrice", "L3.AskSize",
                                       "L5.BidPrice", "L5.BidSize", "L4.AskPrice", "L4.AskSize",
                                       "L6.BidPrice", "L6.BidSize", "L5.AskPrice", "L5.AskSize",
                                       "L7.BidPrice", "L7.BidSize", "L6.AskPrice", "L6.AskSize",
                                       "L8.BidPrice", "L8.BidSize", "L7.AskPrice", "L7.AskSize",
                                       "L9.BidPrice", "L9.BidSize", "L8.AskPrice", "L8.AskSize",
                                       "L10.BidPrice", "L10.BidSize", "L9.AskPrice", "L9.AskSize")],
                        "L11.BidPrice"=as.numeric(NA), "L11.BidSize"=as.numeric(NA),
                        depthBefore[, c("L10.AskPrice", "L10.AskSize")])
  }
  if (side=="A"){
    depthAfter <- cbind(depthBefore[,c("L1.BidPrice", "L1.BidSize", "L2.AskPrice", "L2.AskSize",
                                       "L2.BidPrice", "L2.BidSize", "L3.AskPrice", "L3.AskSize",
                                       "L3.BidPrice", "L3.BidSize", "L4.AskPrice", "L4.AskSize",
                                       "L4.BidPrice", "L4.BidSize", "L5.AskPrice", "L5.AskSize",
                                       "L5.BidPrice", "L5.BidSize", "L6.AskPrice", "L6.AskSize",
                                       "L6.BidPrice", "L6.BidSize", "L7.AskPrice", "L7.AskSize",
                                       "L7.BidPrice", "L7.BidSize", "L8.AskPrice", "L8.AskSize",
                                       "L8.BidPrice", "L8.BidSize", "L9.AskPrice", "L9.AskSize",
                                       "L9.BidPrice", "L9.BidSize", "L10.AskPrice", "L10.AskSize",
                                       "L10.BidPrice", "L10.BidSize")],
                        "L11.AskPrice"=as.numeric(NA), "L11.AskSize"=as.numeric(NA))
  }
  colnames(depthAfter) <- c("L1.BidPrice", "L1.BidSize", "L1.AskPrice", "L1.AskSize",
                            "L2.BidPrice", "L2.BidSize", "L2.AskPrice", "L2.AskSize",
                            "L3.BidPrice", "L3.BidSize", "L3.AskPrice", "L3.AskSize",
                            "L4.BidPrice", "L4.BidSize", "L4.AskPrice", "L4.AskSize",
                            "L5.BidPrice", "L5.BidSize", "L5.AskPrice", "L5.AskSize",
                            "L6.BidPrice", "L6.BidSize", "L6.AskPrice", "L6.AskSize",
                            "L7.BidPrice", "L7.BidSize", "L7.AskPrice", "L7.AskSize",
                            "L8.BidPrice", "L8.BidSize", "L8.AskPrice", "L8.AskSize",
                            "L9.BidPrice", "L9.BidSize", "L9.AskPrice", "L9.AskSize",
                            "L10.BidPrice", "L10.BidSize", "L10.AskPrice", "L10.AskSize")
  return(depthAfter)
}

# Function to append column-----
appendCol <- function(data, col2append, file2write){
  if (!file.exists(file2write)){
    # If file doesn't exist, only save data
    gzcp0(fileDir = file2write, Cont = data)
  } else {
    # If file exist, moving on
    # Read file
    dataOld <- fread(input = paste0("7z x -so ", file2write), header = TRUE, check.names=T)
    if(!col2append %in% colnames(dataOld)) {
      # If the column hasn't beed attached
      # Attach column
      dataNew <- cbind(dataOld,data[,..col2append])
      # Save new file
      gzcp0(fileDir = file2write, Cont = dataNew)
    }
  }
}

# Function to match trades and depth for spread trades-------------------
tdComMatch = function(comTrade, comDepth, output){
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
  # Trade is the data for trades
  # Depth is the data for 5-level depths  
  if (nrow(comTrade)==0) return(paste0("No trades for ", comTrade$X.RIC[1]," on ", comTrade$Date[1]))
  if (nrow(comDepth)==0) return(paste0("No trades for ", comDepth$X.RIC[1]," on ", comDepth$Date[1]))
  # print(paste(comTrade$X.RIC[1], comTrade$Date[1], length(which(is.na(comTrade$L1.BidPrice)))))
  
  
  ##----------- 1.a.Simple exact match --------------------
  # Direct match with time interval, vol, and price
  # If no match, considering split trades
  
  # Matching with interval between chg. of depth where the trade happens
  for (i in 1: nrow(comTrade)) {
    # Skip matched lines
    if (!is.na(comTrade$L1.BidPrice[i])) next
    
    # Find the index of depth change by same date, same time interval between chg. of depth, price, and vol.
    indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                          comDepth$Date == comTrade$Date[i]&
                          comDepth$Second <= comTrade$Second[i]&
                          (comDepth$Second - comTrade$Second[i])==max((comDepth$Second - comTrade$Second[i])[comDepth$Second - comTrade$Second[i]<0])&
                          ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                             (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])))
    # indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
    #                       comDepth$Date == comTrade$Date[i]&
    #                       ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
    #                          (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])))
    # 
    # indexDepth <- indexDepth[comDepth$Second[indexDepth] - comTrade$Second[i] == max((comDepth$Second[indexDepth] - comTrade$Second[i])[(comDepth$Second[indexDepth] - comTrade$Second[i])<0])]
    
    if (length(indexDepth)==1) {
      indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
      comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
      comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
    } else {
      # If there is no fully match, try to find match of split trades with same date, same time interval between chg. of depth, and price
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second <= comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])==max((comDepth$Second - comTrade$Second[i])[comDepth$Second - comTrade$Second[i]<0])&
                            (comDepth$L1.BidPrice==comTrade$Price[i] | comDepth$L1.AskPrice==comTrade$Price[i] ))  
      # indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i] &
      #                       comDepth$Date == comTrade$Date[i] &
      #                       (comDepth$L1.BidPrice==comTrade$Price[i] | comDepth$L1.AskPrice==comTrade$Price[i]))
      # indexDepth <- indexDepth[comDepth$Second[indexDepth] - comTrade$Second[i] == max((comDepth$Second[indexDepth] - comTrade$Second[i])[(comDepth$Second[indexDepth] - comTrade$Second[i])<0])]
      
      if (length(indexDepth)==1) {
        # If there is any match, find the series trades potentially from the same chg. of depth and same price
        indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                              comTrade$Date==comTrade$Date[i] &
                              comTrade$Second>=comTrade$Second[i] &
                              comTrade$Second<=comDepth$Second[indexDepth+1] &
                              comTrade$Price==comTrade$Price[i])
        # Find the matches adding up the the vol. chg. of depth
        splitIndex <- which((comTrade$Price[i]==comDepth$L1.BidPrice[indexDepth] & 
                               cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgBid[indexDepth])
                            |(comTrade$Price[i]==comDepth$L1.AskPrice[indexDepth] & 
                                cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgAsk[indexDepth]))
        splitIndex <- splitIndex[which(indexTrade[splitIndex]>=i)] 
        if (length(splitIndex)>0) {
          comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
          comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
        }
      }
      indexDepth <- NULL # Reset the index
    }
  }
  
  ##----------- 1.b.Simple around match --------------------
  # If no match, considering split trades
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    
    # For those without match, try again using around time
    # According to time error of +/-1 sec
    for (i in which(is.na(comTrade$L1.BidPrice)|is.null(comTrade$L1.BidPrice))) {
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])) next
      
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]&
                            comDepth$Date==comTrade$Date[i]&
                            comDepth$Second-comTrade$Second[i] >= -0.5&
                            comDepth$Second-comTrade$Second[i] <= 0.5&
                            ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])))
      
      if (length(indexDepth)==1) {
        indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
        comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      } else {
        # For those without match, try again by searching Depth_{t-1}
        # Finding close time match in depth
        indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]&
                              comDepth$Date==comTrade$Date[i]&
                              comDepth$Second-comTrade$Second[i] >= -0.5&
                              comDepth$Second-comTrade$Second[i] <= 0.5&
                              ((shift(comDepth$L1.BidPrice, n=1L, type="lag")==comTrade$Price[i] & shift(comDepth$chgBid, n=1L, type="lag")==comTrade$Volume[i]) |
                                 (shift(comDepth$L1.AskPrice, n=1L, type="lag")==comTrade$Price[i] & shift(comDepth$chgAsk, n=1L, type="lag")==comTrade$Volume[i])))
        
        if (length(indexDepth)==1){
          comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth-1, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
          comTrade[i][,(L10Depths)] <- comDepth[indexDepth-1, ..L10Depths]
        } else {
          # Considering split orders around time
          indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]&
                                comDepth$Date==comTrade$Date[i]&
                                comDepth$Second-comTrade$Second[i] >= -0.5&
                                comDepth$Second-comTrade$Second[i] <= 0.5&
                                ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid>=comTrade$Volume[i]) |
                                   (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk>=comTrade$Volume[i])))
          # If more than 1 index matches
          if (length(indexDepth)==1) {
            # If there is any match, find the series trades potentially from the same chg. of depth and same price
            indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                                  comTrade$Date==comTrade$Date[i] &
                                  comTrade$Second>=comTrade$Second[i] &
                                  comTrade$Second<=comDepth$Second[indexDepth+1] &
                                  comTrade$Price==comTrade$Price[i])
            splitIndex <- which((comTrade$Price[i]==comDepth$L1.BidPrice[indexDepth] &
                                   cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgBid[indexDepth]) |
                                  (comTrade$Price[i]==comDepth$L1.AskPrice[indexDepth] &
                                     cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgAsk[indexDepth]))
            splitIndex <- splitIndex[which(indexTrade[splitIndex]>=i)]
            if (length(splitIndex)>0) {
              # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
              comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
              break}
          }
        }
      }
    }
  }
  
  # Previous depth x split orders
  # According to time error of +/-1 sec
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    
    for (i in which(is.na(comTrade$L1.BidPrice))) {
      
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])) next
      
      # Finding close time match in depth
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]&
                            comDepth$Date==comTrade$Date[i]&
                            comDepth$Second-comTrade$Second[i] >= -0.5&
                            comDepth$Second-comTrade$Second[i] <= 0.5)
      
      if (length(indexDepth)==0) next
      
      for (d in indexDepth) {
        indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                              comTrade$Date==comTrade$Date[i] &
                              comTrade$Second>=comTrade$Second[i] &
                              comTrade$Second<=comDepth$Second[d+1] &
                              comTrade$Price==comTrade$Price[i])
        # If agg. depth change matches
        splitIndex <- which((comTrade$Price[i]==comDepth$L1.BidPrice[d-1] &
                               cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgBid[d-1]) |
                              (comTrade$Price[i]==comDepth$L1.AskPrice[d-1] &
                                 cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])==comDepth$chgAsk[d-1]))
        splitIndex <- splitIndex[which(indexTrade[splitIndex]>=i)]
        if (length(splitIndex)>0) {
          # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
          comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[d-1, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
          comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[d-1, ..L10Depths]
          break}
      }
      # }
    }
  }
  
  ##----------- 1.c.New enter orders --------------------
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    for (i in which(is.na(comTrade$L1.BidPrice))) {
      
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])) next
      
      indexDepth <- which(comDepth$X.RIC==comTrade$X.RIC[i] &
                            comDepth$Date==comTrade$Date[i] &
                            comDepth$Second - comTrade$Second[i]==max((comDepth$Second - comTrade$Second[i])[(comDepth$Second - comTrade$Second[i])<0]) &
                            ((comDepth$L1.BidPrice==comTrade$Price[i] &  shift(comDepth$L2.BidPrice, n=1L, type = "lead")==comTrade$Price[i] &
                                shift(comDepth$L2.BidSize, n=1L, type = "lead")==comDepth$L1.BidSize - comTrade$Volume[i])|
                               (comDepth$L1.AskPrice==comTrade$Price[i] &  shift(comDepth$L2.AskPrice, n=1L, type = "lead")==comTrade$Price[i] &
                                  shift(comDepth$L2.AskSize, n=1L, type = "lead")==comDepth$L1.AskSize - comTrade$Volume[i])))
      # Situation where Price_t = L2.quote_{t+1}
      
      if (length(indexDepth)==1) {
        if (!is.na(comDepth$L2.BidPrice[indexDepth+1]) & !is.na(comDepth$L2.BidSize[indexDepth+1])) {
          if (comDepth$L1.BidPrice[indexDepth]==comTrade$Price[i] &
              comDepth$L2.BidPrice[indexDepth+1]==comTrade$Price[i] &
              comDepth$L2.BidSize[indexDepth+1]==comDepth$L1.BidSize[indexDepth] - comTrade$Volume[i]) {
            # If bid side matches
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice")],
                                                                              comDepth[indexDepth, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth+1, c("L2.BidSize","L1.AskSize")])
          }
        } else {
          # Otherwise ask side matches
          comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice")],
                                                                            comDepth[indexDepth, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth+1, c("L1.BidSize","L2.AskSize")])
        }
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      } else {
        # For those without match, try again using around time
        indexDepth <-  which(comDepth$Date.L.==comTrade$Date.L.[i]&
                               comDepth$Second-comTrade$Second[i] >-0.5 & comDepth$Second-comTrade$Second[i] < 0.5&
                               ((comDepth$L1.BidPrice==comTrade$Price[i] &  shift(comDepth$L2.BidPrice, n=1L, type = "lead")==comTrade$Price[i] &
                                   shift(comDepth$L2.BidSize, n=1L, type = "lead")==comDepth$L1.BidSize - comTrade$Volume[i])|
                                  (comDepth$L1.AskPrice==comTrade$Price[i] &  shift(comDepth$L2.AskPrice, n=1L, type = "lead")==comTrade$Price[i] &
                                     shift(comDepth$L2.AskSize, n=1L, type = "lead")==comDepth$L1.AskSize - comTrade$Volume[i])))
        # Situation where Price_t = L2.quote_{t+1}
        
        if (length(indexDepth)==1) {
          if (!is.na(comDepth$L2.BidPrice[indexDepth+1]) & !is.na(comDepth$L2.BidSize[indexDepth+1])) {
            if (comDepth$L1.BidPrice[indexDepth]==comTrade$Price[i] &
                comDepth$L2.BidPrice[indexDepth+1]==comTrade$Price[i] &
                comDepth$L2.BidSize[indexDepth+1]==comDepth$L1.BidSize[indexDepth] - comTrade$Volume[i]) {
              # If bid side matches
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice")],
                                                                                comDepth[indexDepth, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth+1, c("L2.BidSize","L1.AskSize")])
            }
          } else {
            # Otherwise ask side matches
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice")],
                                                                              comDepth[indexDepth, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth+1, c("L1.BidSize","L2.AskSize")])
          }
          comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
        } 
        if (length(indexDepth)==0){
          # For those without match, try again using around time to Depth_{t-1}
          
          indexDepth <-  which(comDepth$Date.L.==comTrade$Date.L.[i]&
                                 comDepth$Second-comTrade$Second[i] >-0.5 & comDepth$Second-comTrade$Second[i] < 0.5&
                                 ((comDepth$L2.BidPrice==comTrade$Price[i] &  shift(comDepth$L1.BidPrice, n=1L, type = "lag")==comTrade$Price[i] &
                                     shift(comDepth$L1.BidSize, n=1L, type = "lag")==comDepth$L2.BidSize + comTrade$Volume[i])|
                                    (comDepth$L2.AskPrice==comTrade$Price[i] &  shift(comDepth$L1.AskPrice, n=1L, type = "lag")==comTrade$Price[i] &
                                       shift(comDepth$L1.AskSize, n=1L, type = "lag")==comDepth$L2.AskSize + comTrade$Volume[i])))
          
          if (length(indexDepth)==1){
            if(!is.na(comDepth$L2.BidPrice[indexDepth]) & !is.na(comDepth$L2.BidSize[indexDepth])) {
              if (comDepth$L1.BidPrice[indexDepth-1]==comTrade$Price[i] &
                  comDepth$L2.BidPrice[indexDepth]==comTrade$Price[i] &
                  comDepth$L2.BidSize[indexDepth]==comDepth$L1.BidSize[indexDepth] - comTrade$Volume[i]) {
                comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth-1, c("L1.BidPrice","L1.AskPrice")],
                                                                                  comDepth[indexDepth-1, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth, c("L2.BidSize","L1.AskSize")])
              }
            } else {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[indexDepth-1, c("L1.BidPrice","L1.AskPrice")],
                                                                                comDepth[indexDepth-1, c("L1.BidSize","L1.AskSize")] - comDepth[indexDepth, c("L1.BidSize","L2.AskSize")])
            }
            comTrade[i][,(L10Depths)] <- comDepth[indexDepth-1, ..L10Depths]
          }
        }
      }
    }
  }
  
  # Deal with split x new enter orders
  # Assuming the trades do not necessarily exe. around time
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    for (i in which(is.na(comTrade$L1.BidPrice))) {
      
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])| is.na(comTrade$Price[i])) next
      
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i] &
                            comDepth$Date==comTrade$Date[i] &
                            comDepth$Second-comTrade$Second[i] >-0.5 & comDepth$Second-comTrade$Second[i] < 0.5 &
                            ((comDepth$L1.BidPrice==comTrade$Price[i] &  shift(comDepth$L2.BidPrice, n=1L, type = "lead")==comTrade$Price[i])|
                               (comDepth$L1.AskPrice==comTrade$Price[i] &  shift(comDepth$L2.AskPrice, n=1L, type = "lead")==comTrade$Price[i])))
      
      if (length(indexDepth)==0) next
      for (d in indexDepth) {
        # Find split order, judge by trading time and price
        indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                              comTrade$Date==comTrade$Date[i] &
                              comTrade$Second>=comTrade$Second[i] &
                              comTrade$Second<=comDepth$Second[d+1] &
                              comTrade$Price==comTrade$Price[i])
        # If agg. depth change matches, by price, old depth and new depth
        vol <- cumsum(comTrade$Volume[indexTrade][comTrade$Price[indexTrade]==comTrade$Price[i]])
        splitIndex <- which((comTrade$X.RIC==comTrade$X.RIC[i] &
                               comTrade$Date==comTrade$Date[i] &
                               comTrade$Price[i]==comDepth$L1.BidPrice[d] &
                               comTrade$Price[i]==comDepth$L2.BidPrice[d+1] &
                               vol == comDepth$L1.BidPrice[d] - comDepth$L2.BidPrice[d+1]) |
                              (comTrade$X.RIC==comTrade$X.RIC[i] &
                                 comTrade$Date==comTrade$Date[i] &
                                 comTrade$Price[i]==comDepth$L1.AskPrice[d] &
                                 comTrade$Price[i]==comDepth$L2.AskPrice[d+1] &
                                 vol==comDepth$L1.AskSize[d] - comDepth$L2.AskSize[d+1]))
        if (length(splitIndex)==1) {
          if ((comDepth$L1.BidPrice==comTrade$Price[i] &
               comDepth$L1.BidPrice[d]==comDepth$L2.BidPrice[d+1])) {
            comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d, c("L1.BidPrice","L1.AskPrice")],
                                                                                                     comDepth[d, c("L1.BidSize","L1.AskSize")] - comDepth[d+1, c("L2.BidSize","L1.AskSize")])
          } else {
            comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d, c("L1.BidPrice","L1.AskPrice")],
                                                                                                     comDepth[d, c("L1.BidSize","L1.AskSize")] - comDepth[d+1, c("L1.BidSize","L2.AskSize")])
          }
          # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
          comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[d, ..L10Depths]
          break
        } else {
          # Trying last depth, Price_t = L1.Quote_{t-1}=L2.Quote_{t}
          splitIndex <- which((comTrade$X.RIC==comTrade$X.RIC[i] &
                                 comTrade$Date==comTrade$Date[i] &
                                 comTrade$Price[i]==comDepth$L1.BidPrice[d-1] &
                                 comTrade$Price[i]==comDepth$L2.BidPrice[d] &
                                 vol==comDepth$L1.BidSize[d-1]-comDepth$L2.BidSize[d]) |
                                (comTrade$X.RIC==comTrade$X.RIC[i] &
                                   comTrade$Date==comTrade$Date[i] &
                                   comTrade$Price[i]==comDepth$L1.AskPrice[d-1] &
                                   comTrade$Price[i]==comDepth$L2.AskPrice[d] &
                                   vol==comDepth$L1.AskSize[d-1]-comDepth$L2.AskSize[d]))
          if (length(splitIndex)==1) {
            # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
            if ((comDepth$L1.BidPrice==comTrade$Price[i] &
                 comDepth$L1.BidPrice[d-1]==comDepth$L2.BidPrice[d])) {
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d-1, c("L1.BidPrice","L1.AskPrice")],
                                                                                                       comDepth[d-1, c("L1.BidSize","L1.AskSize")] - comDepth[d, c("L2.BidSize","L1.AskSize")])
            } else {
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d-1, c("L1.BidPrice","L1.AskPrice")],
                                                                                                       comDepth[d-1, c("L1.BidSize","L1.AskSize")] - comDepth[d, c("L1.BidSize","L2.AskSize")])
            }
            comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[d-1, ..L10Depths]
            break
          }
        }
      }
    }
  }
  
  ##--------- 1.d.Special match for split order, ignore the price in split trades-----
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    for (i in which(is.na(comTrade$L1.BidPrice))) {
      
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])) next
      
      indexDepth <- which(comDepth$X.RIC==comTrade$X.RIC[i] &
                            comDepth$Date==comTrade$Date[i] &
                            comDepth$Second-comTrade$Second[i] >-0.5 & comDepth$Second-comTrade$Second[i] < 0.5)
      
      if (length(indexDepth)==0) next
      for (d in indexDepth) {
        # Find split order, judge by trading time and price
        indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                              comTrade$Date==comTrade$Date[i] &
                              comTrade$Second>=comTrade$Second[i] &
                              comTrade$Second<=comDepth$Second[d+1])
        # If agg. depth change matches, by price, old depth and new depth
        vol <- cumsum(comTrade$Volume[indexTrade])
        splitIndex <- which((comTrade$X.RIC==comTrade$X.RIC[i] &
                               comTrade$Date==comTrade$Date[i] &
                               comTrade$Price[i]==comDepth$L1.BidPrice[d] &
                               vol==comDepth$chgBid[d]) |
                              (comTrade$X.RIC==comTrade$X.RIC[i] &
                                 comTrade$Date==comTrade$Date[i] &
                                 comTrade$Price[i]==comDepth$L1.AskPrice[d] &
                                 vol==comDepth$chgAsk[d]))
        if (length(splitIndex)==1) {
          # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
          comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[d, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
          comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[d, ..L10Depths]
          break
        } else {
          # Trying last depth
          splitIndex <- which((comTrade$X.RIC==comTrade$X.RIC[i] &
                                 comTrade$Date==comTrade$Date[i] &
                                 comTrade$Price[i]==comDepth$L1.BidPrice[d-1] &
                                 vol==comDepth$chgBid[d-1]) |
                                (comTrade$X.RIC==comTrade$X.RIC[i] &
                                   comTrade$Date==comTrade$Date[i] &
                                   comTrade$Price[i]==comDepth$L1.AskPrice[d-1] &
                                   vol==comDepth$chgAsk[d-1]))
          if (length(splitIndex)==1) {
            # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
            comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[d-1, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
            comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- comDepth[d-1, ..L10Depths]
            break
          }
        }
      }
    }
  }
  
  ##---------1.e.Special match through depth levels -----
  # Happens only when the level1 depth is fully taken
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    for (i in which(is.na(comTrade$L1.BidPrice))) {
      
      # Skip matched lines
      if (!is.na(comTrade$L1.BidPrice[i])) next
      
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i] &
                            comDepth$Date==comTrade$Date[i] &
                            comDepth$Second-comTrade$Second[i] >-0.5 & comDepth$Second-comTrade$Second[i] < 0.5)
      
      if (length(indexDepth)==0) next
      
      for (d in indexDepth[indexDepth!=1 & indexDepth!=nrow(comDepth)]) {
        # If the bid side trade through
        # IndexDepth matched with trade time before, i.e. Price_t = P2_{t-1}
        if (!is.na(comDepth$L2.BidPrice[d-1]) & !is.na(comDepth$L1.BidPrice[d]) & !is.na(comTrade$Price[i])) {
          if (comTrade$Price[i] == comDepth$L2.BidPrice[d-1] &
              comDepth$L1.BidPrice[d]==comDepth$L2.BidPrice[d-1] &
              comDepth$L1.BidSize[d] < comDepth$L2.BidSize[d-1]) {
            indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                                  comTrade$Date==comTrade$Date[i] &
                                  comTrade$Second>=comTrade$Second[i] &
                                  comTrade$Second<=comDepth$Second[d+1] &
                                  comTrade$Price==comDepth$L2.BidPrice[d-1])
            indexTrade <- indexTrade[indexTrade>=i]
            vol <- cumsum(comTrade$Volume[indexTrade])
            # If agg. depth change matches, by price, old depth and new depth
            splitIndex <- which(comTrade[indexTrade]$X.RIC==comTrade$X.RIC[i] &
                                  comTrade[indexTrade]$Date==comTrade$Date[i] &
                                  comTrade$Price[i]==comDepth$L2.BidPrice[d-1] &
                                  vol==comDepth$L2.BidSize[d-1] - comDepth$L1.BidSize[d])
            if (length(splitIndex)==1) {
              # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d-1, c("L2.BidPrice","L1.AskPrice")],
                                                                                                       comDepth[d-1, c("L2.BidSize","L1.AskSize")]-comDepth[d, c("L1.BidSize","L1.AskSize")])
              comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- ttDepth("B", comDepth[d-1, ..L10Depths])
              break
            }
          } 
        }
        
        # Skip matched lines
        if (!is.na(comTrade$L1.AskPrice[i]) | is.na(comTrade$Price[i])) next
        # If the bid side trade through
        # IndexDepth matched with trade before, i.e. Price_t = P2_{t-1}
        if (!is.na(comDepth$L2.AskPrice[d-1]) & !is.na(comDepth$L1.AskPrice[d]) & !is.na(comTrade$Price[i])) {
          if (comTrade$Price[i] == comDepth$L2.AskPrice[d-1] &
              comDepth$L1.AskPrice[d]==comDepth$L2.AskPrice[d-1] &
              comDepth$L1.AskSize[d]<comDepth$L2.AskSize[d-1]) {
            indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                                  comTrade$Date==comTrade$Date[i] &
                                  comTrade$Second>=comTrade$Second[i] &
                                  comTrade$Second<=comDepth$Second[d+1] &
                                  comTrade$Price==comDepth$L2.AskPrice[d-1])
            indexTrade <- indexTrade[indexTrade>=i]
            vol <- cumsum(comTrade$Volume[indexTrade])
            # If agg. depth change matches, by price, old depth and new depth
            splitIndex <- which(comTrade[indexTrade]$X.RIC==comTrade$X.RIC[i] &
                                  comTrade[indexTrade]$Date==comTrade$Date[i] &
                                  comTrade$Price[i]==comDepth$L2.AskPrice[d-1] &
                                  vol==comDepth$L2.AskSize[d-1] - comDepth$L1.AskSize[d])
            if (length(splitIndex)==1) {
              # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d-1, c("L1.BidPrice","L2.AskPrice")],
                                                                                                       comDepth[d-1, c("L1.BidSize","L2.AskSize")]-comDepth[d, c("L1.BidSize","L1.AskSize")])
              
              comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- ttDepth("A", comDepth[d-1, ..L10Depths])
              break
            }
          }
        }
        
        # Skip matched lines
        if (!is.na(comTrade$L1.BidPrice[i])) next
        # If the bid side trade through
        # IndexDepth matched with trade time, i.e. Price_t = P2_{t}
        if (!is.na(comDepth$L2.BidPrice[d]) & !is.na(comDepth$L1.BidPrice[d+1]) & !is.na(comTrade$Price[i])) {
          if (comDepth$L2.BidPrice[d]==comTrade$Price[i]&
              comDepth$L2.BidPrice[d]==comDepth$L1.BidPrice[d+1] &
              comDepth$L2.BidSize[d]>comDepth$L1.BidSize[d+1]) {
            indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                                  comTrade$Date==comTrade$Date[i] &
                                  comTrade$Second>=comTrade$Second[i] &
                                  comTrade$Second<=comDepth$Second[d+1] &
                                  comTrade$Price==comDepth$L2.BidPrice[d])
            indexTrade <- indexTrade[indexTrade>=i]
            vol <- cumsum(comTrade$Volume[indexTrade])
            # If agg. depth change matches, by price, old depth and new depth
            splitIndex <-  which(comTrade[indexTrade]$X.RIC==comTrade$X.RIC[i] &
                                   comTrade[indexTrade]$Date==comTrade$Date[i] &
                                   comTrade$Price[i]==comDepth$L2.BidPrice[d] &
                                   vol==comDepth$L2.BidSize[d] - comDepth$L1.BidSize[d+1])
            if (length(splitIndex)==1) {
              # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d, c("L2.BidPrice","L1.AskPrice")],
                                                                                                       comDepth[d, c("L2.BidSize","L1.AskSize")]-comDepth[d+1, c("L1.BidSize","L1.AskSize")])
              
              comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- ttDepth("B", comDepth[d, ..L10Depths])
              break
            }
          } 
        }
        
        # Skip matched lines
        if (!is.na(comTrade$L1.BidPrice[i])) next
        # If the bid side trade through
        # IndexDepth matched with trade time, i.e. Price_t = P2_{t}
        if (!is.na(comDepth$L2.AskPrice[d]) & !is.na(comDepth$L1.AskPrice[d+1]) & !is.na(comTrade$Price[i])) {
          if (comDepth$L2.AskPrice[d]==comTrade$Price[i] &
              comDepth$L2.AskPrice[d]==comDepth$L1.AskPrice[d+1] &
              comDepth$L2.AskSize[d]>comDepth$L1.AskSize[d+1]) {
            indexTrade <- which(comTrade$X.RIC==comTrade$X.RIC[i] &
                                  comTrade$Date==comTrade$Date[i] &
                                  comTrade$Second>=comTrade$Second[i] &
                                  comTrade$Second<=comDepth$Second[d+1] &
                                  comTrade$Price==comDepth$L2.AskPrice[d])
            indexTrade <- indexTrade[indexTrade>=i]
            vol <- cumsum(comTrade$Volume[indexTrade])
            # If agg. depth change matches, by price, old depth and new depth
            splitIndex <- which(comTrade[indexTrade]$X.RIC==comTrade$X.RIC[i] &
                                  comTrade[indexTrade]$Date==comTrade$Date[i] &
                                  comTrade$Price[i]==comDepth$L2.AskPrice[d] &
                                  vol==comDepth$L2.AskSize[d] - comDepth$L1.AskSize[d+1])
            if (length(splitIndex)==1) {
              # i <- which(is.na(comTrade$L1.BidPrice))[which(which(is.na(comTrade$L1.BidPrice))==i) + splitIndex] # Move on before fill in the match
              comTrade[indexTrade[1:splitIndex], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- cbind(comDepth[d, c("L1.BidPrice","L2.AskPrice")],
                                                                                                       comDepth[d, c("L1.BidSize","L2.AskSize")]-comDepth[d+1, c("L1.BidSize","L1.AskSize")])
              
              comTrade[indexTrade[1:splitIndex]][,(L10Depths)] <- ttDepth("A", comDepth[d, ..L10Depths])
              break
            }
          }
        }
      }
    }
  }
  
  ##-------1.f. Final approx. match by last depth -------
  if (length(which(is.na(comTrade$L1.BidPrice)))>0) {
    matchIndex <- unlist(sapply(comTrade$Second[which(is.na(comTrade$L1.BidPrice))], closestDepth, depth=comDepth$Second, tradeDate=comTrade$Date[which(is.na(comTrade$L1.BidPrice))]))
    if (length(matchIndex)>0) {
      comTrade[which(is.na(comTrade$L1.BidPrice))[which(!is.na(matchIndex))], c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[matchIndex[which(!is.na(matchIndex))], c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
      comTrade[which(is.na(comTrade$L1.BidPrice))[which(!is.na(matchIndex))]][,(L10Depths)] <- comDepth[matchIndex[which(!is.na(matchIndex))], ..L10Depths]
    }
  }
  
  comTrade <- comTrade[Price==bestBid, initiate := "Sell"][Price==bestAsk, initiate := "Buy"]
  
  # return(comTrade)
  
  # Check matching results
  # print(paste(comTrade$X.RIC[1], comTrade$Date[1], length(which(is.na(comTrade$L1.BidPrice))), "trade(s) with no match"))
  fwrite(comTrade,file = output, row.names=FALSE, col.names = !file.exists(output), sep=",", append = TRUE)
}

# Function to match spreads and regular trades and depth -----------------------
srTDMatch = function(comTrade, regTradeDir, regDepthDir) {
  
  # Create file path for merged spread and regular trades
  comRegDir <- gsub(tail(strsplit(regTradeDir,split = "/")[[1]],1),"comRegTD",regTradeDir)
  dir.create(comRegDir, showWarnings = FALSE)
  # Create file path for indexed regular trades
  regTDDir <- gsub(tail(strsplit(regTradeDir,split = "/")[[1]],1),"RegDepthTAQ",regTradeDir)
  dir.create(regTDDir, showWarnings = FALSE)
  
  if (nrow(comTrade)==0) return(paste0("No trades for ", comTrade$X.RIC[1]," on ", comTrade$Date[1]))
  
  # --------- 2.a.Load regular trades data ---------
  # Match combo with regular ones
  spreadRIC <- comTrade$X.RIC[1]
  spreadDate <- comTrade$Date[1]
  shortRIC <- substr(spreadRIC, 1, 8)
  longRIC <- paste0(substr(spreadRIC, 1, 6),substr(spreadRIC, 10, 11))
  
  # File dir of trade data, using original direction
  longTDfile <- paste0(regTradeDir, longRIC, "_", spreadDate, ".csv.gz")
  shortTDfile <- paste0(regTradeDir, shortRIC, "_", spreadDate, ".csv.gz")
  
  # Load regular trading data, set time format
  # Meanwhile drop trades outside pre-trading, continuous trading, and post-trading sessions
  longTrade <- loadTrades(longTDfile)
  shortTrade <- loadTrades(shortTDfile)
  
  #--------- 2.b.Match reg depth with reg trades-------------
  if (nrow(longTrade)==0) {
    longTrade <- comTrade[, c("X.RIC","Type","Price","Volume","Date","Time","Second")]
    longTrade$X.RIC <- longRIC
    longTrade$Type <- "Trade"
    longTrade$Price <- NA
    longTrade$Volume <- NA
  }
  longTrade <- tdRegMatch(longTrade, folderDepth = regDepthDir)
  shortTrade <- tdRegMatch(shortTrade, folderDepth = regDepthDir)
  
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
  longL10Depths <- c("longL1.BidPrice", "longL1.BidSize", "longL1.AskPrice", "longL1.AskSize",
                     "longL2.BidPrice", "longL2.BidSize", "longL2.AskPrice", "longL2.AskSize",
                     "longL3.BidPrice", "longL3.BidSize", "longL3.AskPrice", "longL3.AskSize",
                     "longL4.BidPrice", "longL4.BidSize", "longL4.AskPrice", "longL4.AskSize",
                     "longL5.BidPrice", "longL5.BidSize", "longL5.AskPrice", "longL5.AskSize",
                     "longL6.BidPrice", "longL6.BidSize", "longL6.AskPrice", "longL6.AskSize",
                     "longL7.BidPrice", "longL7.BidSize", "longL7.AskPrice", "longL7.AskSize",
                     "longL8.BidPrice", "longL8.BidSize", "longL8.AskPrice", "longL8.AskSize",
                     "longL9.BidPrice", "longL9.BidSize", "longL9.AskPrice", "longL9.AskSize",
                     "longL10.BidPrice", "longL10.BidSize", "longL10.AskPrice", "longL10.AskSize")
  
  shortL10Depths <- c("shortL1.BidPrice", "shortL1.BidSize", "shortL1.AskPrice", "shortL1.AskSize",
                      "shortL2.BidPrice", "shortL2.BidSize", "shortL2.AskPrice", "shortL2.AskSize",
                      "shortL3.BidPrice", "shortL3.BidSize", "shortL3.AskPrice", "shortL3.AskSize",
                      "shortL4.BidPrice", "shortL4.BidSize", "shortL4.AskPrice", "shortL4.AskSize",
                      "shortL5.BidPrice", "shortL5.BidSize", "shortL5.AskPrice", "shortL5.AskSize",
                      "shortL6.BidPrice", "shortL6.BidSize", "shortL6.AskPrice", "shortL6.AskSize",
                      "shortL7.BidPrice", "shortL7.BidSize", "shortL7.AskPrice", "shortL7.AskSize",
                      "shortL8.BidPrice", "shortL8.BidSize", "shortL8.AskPrice", "shortL8.AskSize",
                      "shortL9.BidPrice", "shortL9.BidSize", "shortL9.AskPrice", "shortL9.AskSize",
                      "shortL10.BidPrice", "shortL10.BidSize", "shortL10.AskPrice", "shortL10.AskSize")
  
  #--------- 2.c.Match spreads trades with reg trades-------------
  # Fixing mismatching later trades
  # and allowing for split trades
  longTrade$matchedLong <- 0
  shortTrade$matchedShort <- 0
  comTrade[,(longL10Depths)] <- numeric(0)
  comTrade[,(shortL10Depths)] <- numeric(0)
  for (i in 1:nrow(comTrade)) { #nrow(comTrade)
    
    if (i <= max(longTrade$matchedLong)) next
    if (i >nrow(comTrade)) break
    timeDiffL <- max(0.5, min.n(longTrade$Second[longTrade$matchedLong==0 & longTrade$Second>=comTrade$Second[i]],5) - comTrade$Second[i])
    timeDiffS <- max(0.5, min.n(shortTrade$Second[shortTrade$matchedShort==0 & shortTrade$Second>=comTrade$Second[i]],5) - comTrade$Second[i])
    
    # Find index of both legs by closest time
    longIndex <- which(longTrade$Date ==comTrade$Date[i] &
                         abs(longTrade$Second - comTrade$Second[i])<=timeDiffL &
                         longTrade$Volume==comTrade$Volume[i] &
                         longTrade$matchedLong==0)
    
    shortIndex <- which(shortTrade$Date ==comTrade$Date[i] &
                          abs(shortTrade$Second - comTrade$Second[i])<= timeDiffS &
                          shortTrade$Volume==comTrade$Volume[i] &
                          shortTrade$matchedShort==0)
    if (i>1) {
      longIndex <- longIndex[which(longIndex>which.max(longTrade$matchedLong))]
      shortIndex <- shortIndex[which(shortIndex>which.max(shortTrade$matchedShort))]
    } 
    
    # Calculate the combined net price and time difference
    spreads <- longTrade[longIndex][, shortIndex, by=longIndex]
    spreads <- spreads[, dPrice := round(longTrade[longIndex]$Price-shortTrade[shortIndex]$Price,2), by=.(longIndex,shortIndex)]
    spreads <- spreads[, dSec := abs(longTrade[longIndex]$Second-shortTrade[shortIndex]$Second), by=.(longIndex,shortIndex)]
    spreads <- spreads[, dSecL := abs(longTrade[longIndex]$Second-comTrade$Second[i])]
    spreads <- spreads[, dSecS := abs(shortTrade[shortIndex]$Second-comTrade$Second[i])]
    spreads <- spreads[which(spreads$dPrice == comTrade$Price[i]),]
    
    if (i>1 & nrow(spreads)>0) {
      spreads <- spreads[which(spreads$dPrice == comTrade$Price[i] &
                                 spreads$dSec <= max(0.1,timeDiffL+timeDiffS) &
                                 spreads$dSecL <= max(0.1,timeDiffL) &
                                 spreads$dSecS <= max(0.1,timeDiffS))]
      spreads <- spreads[order(longIndex,shortIndex),]
      # spreads <- spreads[1,]
      k <- length(which(comTrade$Second>comTrade$Second[i] & comTrade$Second<longTrade$Second[spreads$longIndex[1]] & comTrade$Price==comTrade$Price[i]))
      # Number of combo trades after i as a potential match to long trade
      
      if (nrow(spreads)>0 & k>0) {
        l <- spreads$longIndex[1]-1-which.max(longTrade$matchedLong) # Long trade records between match i and last match
        s <- spreads$shortIndex[1]-1-which.max(shortTrade$matchedShort) # Short trades records between match i and last match
        if (l>0 & s>0 &
            length(intersect(comTrade$Volume[1:k+i], longTrade$Volume[which.max(longTrade$matchedLong)+1:l]))>0 & 
            length(which(abs(comTrade$Second[1:k+i]-longTrade$Second[which.max(longTrade$matchedLong)+1:l])<0.5))>0 & 
            length(intersect(comTrade$Volume[1:k+i], shortTrade$Volume[which.max(shortTrade$matchedShort)+1:s]))>0) {
          spreads <- spreads[0,] # Delete potential match if the combo trade can match with long/short trades after current match
          # # Find matched index for both legs by last trade
          # spreadIndex <- which(spreads$dPrice == comTrade$Price[i] & abs(spreads$dSec)==min(abs(spreads$dSec))
        }
      }
    }
    
    if (nrow(spreads)>0) {
      # Flag matched trades
      longTrade$matchedLong[spreads$longIndex[1]] <- i
      shortTrade$matchedShort[spreads$shortIndex[1]] <- i
      # Combine legs price with combo data
      comTrade[i, c("priceLong","priceShort")] <- cbind.data.frame("priceLong"=longTrade$Price[spreads$longIndex[1]],
                                                                   "priceShort"=shortTrade$Price[spreads$shortIndex[1]])
      # Combine legs depth with combo trades 
      comTrade[i][,(longL10Depths)] <- longTrade[spreads$longIndex[1], ..L10Depths]
      comTrade[i][,(shortL10Depths)] <- shortTrade[spreads$shortIndex[1], ..L10Depths]
      
      # Match depth data for best bid/ask prices
      # comTrade[i, c("priceLong","priceShort","bestBidLong", "bestAskLong", "bestBidShort", "bestAskShort")] <-
      #   cbind(longTrade$Price[spreads$longIndex[spreadIndex]],
      #         longTrade$Price[spreads$shortIndex[spreadIndex]])
    }
    
    # If find no match with the volume, considering split orders
    if ((length(longIndex)==0 | length(shortIndex)==0 | nrow(spreads)==0) & !is.na(comTrade$chgBid[i]) & !is.na(comTrade$chgAsk[i])) {
      cumvol <- c(max(comTrade$chgBid[i], 0), max(comTrade$chgAsk[i], 0))[
        1*(comTrade$Price[i]>(comTrade$bestBid[i]+comTrade$bestAsk[i])/2)+1] # Matching with cum. vol. instead of vol of each trade
      
      childNum <- which(comTrade$Second-comTrade$Second[i]>=0 &
                          comTrade$Second-comTrade$Second[i]<=0.5 &
                          comTrade$Price==comTrade$Price[i] &
                          comTrade$chgBid==comTrade$chgBid[i] &
                          comTrade$chgAsk==comTrade$chgAsk[i] &
                          comTrade$Volume <= cumvol &
                          is.na(comTrade$priceLong))
      childNum <- childNum[which(cumsum(comTrade$Volume[childNum])<= cumvol)] # Index of split spreads trade
      
      longIndex <- which(longTrade$Date ==comTrade$Date[i] &
                           abs(longTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffL) &
                           longTrade$Volume == cumvol &
                           longTrade$matchedLong==0)
      
      shortIndex <- which(shortTrade$Date ==comTrade$Date[i] &
                            abs(shortTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffS) &
                            shortTrade$Volume == cumvol &
                            shortTrade$matchedShort==0)
      if (length(longIndex)==0 | length(shortIndex)==0 | length(childNum)==0) {
        childNum <- which(comTrade$Second-comTrade$Second[i]>=0 &
                            comTrade$Second-comTrade$Second[i]<=0.5 &
                            comTrade$Price==comTrade$Price[i] &
                            comTrade$bestBid==comTrade$bestBid[i] &
                            comTrade$bestAsk==comTrade$bestAsk[i] &
                            is.na(comTrade$priceLong))
        
        cumvol <- cumsum(comTrade$Volume[childNum])
        longIndex <- which(longTrade$Date ==comTrade$Date[i] &
                             abs(longTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffL) &
                             longTrade$matchedLong==0)
        shortIndex <- which(shortTrade$Date ==comTrade$Date[i] &
                              abs(shortTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffS) &
                              shortTrade$matchedShort==0)
        
        cumvol <- intersect(intersect(cumvol,longTrade$Volume[longIndex]),shortTrade$Volume[shortIndex])
        childNum <- childNum[which(cumsum(comTrade$Volume[childNum])<=cumvol)]
        longIndex <- longIndex[which(longTrade$Volume[longIndex] %in% cumvol)]
        shortIndex <-shortIndex[which(shortTrade$Volume[shortIndex] %in% cumvol)]
        
      }
      
      if (length(longIndex)==0 | length(shortIndex)==0) {
        longIndex <- which(longTrade$Date ==comTrade$Date[i] &
                             abs(longTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffL) &
                             longTrade$Volume + shift(longTrade$Volume, n=1L, type = "lead") == cumvol &
                             longTrade$matchedLong==0)
        
        shortIndex <- which(shortTrade$Date ==comTrade$Date[i] &
                              abs(shortTrade$Second - comTrade$Second[i])<=max(0.5, timeDiffS) &
                              shortTrade$Volume + shift(shortTrade$Volume, n=1L, type = "lead") == cumvol &
                              shortTrade$matchedShort==0)
      }
      
      # Calculate the combined net price and time difference
      spreads <- longTrade[longIndex][, shortIndex, by=longIndex]
      spreads <- spreads[, dPrice := round(longTrade[longIndex]$Price-shortTrade[shortIndex]$Price,2), by=.(longIndex,shortIndex)]
      spreads <- spreads[, dSec := longTrade[longIndex]$Second-shortTrade[shortIndex]$Second, by=.(longIndex,shortIndex)]
      spreads <- spreads[which(spreads$dPrice == comTrade$Price[i]),]
      
      if (length(longIndex)>0 & length(shortIndex)>0 & nrow(spreads)>0) {
        # Flag matched trades
        longTrade$matchedLong[spreads$longIndex[1]] <- max(childNum)
        shortTrade$matchedShort[spreads$shortIndex[1]] <- max(childNum)
        i <- max(childNum)+1
        # Combine legs price with combo data
        comTrade[childNum, c("priceLong","priceShort")] <- cbind.data.frame("priceLong"=longTrade$Price[spreads$longIndex[1]],
                                                                            "priceShort"=shortTrade$Price[spreads$shortIndex[1]])
        
        # Combine legs depth with combo trades 
        comTrade[childNum][,(longL10Depths)] <- longTrade[spreads$longIndex[1], ..L10Depths]
        comTrade[childNum][,(shortL10Depths)] <- shortTrade[spreads$shortIndex[1], ..L10Depths]
      } else {
        # print(paste0(comTrade$X.RIC[1],"_",comTrade$Date[1],"_",i))
        longIndex <- closestDepth(comTrade$Second[i],longTrade$Second, comTrade$Date[i])
        shortIndex <- closestDepth(comTrade$Second[i],shortTrade$Second, comTrade$Date[i])
        
        longIndex <- longIndex[which(!is.na(longIndex))]
        shortIndex <- shortIndex [which(!is.na(shortIndex))]
        
        if (length(longIndex)>0 & length(shortIndex)>0) {
          longTrade$matchedLong[longIndex[1]] <- c(i,longTrade$matchedLong[longIndex[1]])[2-(longTrade$matchedLong[longIndex[1]]==0)]
          shortTrade$matchedShort[shortIndex[1]] <- c(i,shortTrade$matchedShort[shortIndex[1]])[2-(shortTrade$matchedShort[shortIndex[1]]==0)]
          # Combine legs price with combo dataa
          comTrade[i, c("priceLong","priceShort")] <- cbind.data.frame("priceLong"=longTrade$Price[longIndex[1]],
                                                                       "priceShort"=shortTrade$Price[shortIndex[1]])
          
          # Combine legs depth with combo trades 
          comTrade[i][,(longL10Depths)] <- longTrade[spreads$longIndex[1], ..L10Depths]
          comTrade[i][,(shortL10Depths)] <- shortTrade[spreads$shortIndex[1], ..L10Depths]
        }
      }
    }
    
    # End of split order
    
  }
  
  # Count number of matching
  # length(which(!is.na(comTrade$priceLong))) # 2552/2552
  # which(is.na(comTrade$priceLong))
  
  # Save results of combined file
  # But Batch works too slow
  # fwrite(comTrade, file = "comRegTD.csv",
  #        row.names=FALSE, col.names = !file.exists("comRegTD.csv"), sep=",", append = FALSE)
  comRegFile <- paste0(comRegDir, comTrade$X.RIC[1],"_",comTrade$Date[1],"_","comRegTD.csv.gz")
  gzcp0(fileDir = comRegFile, Cont = comTrade)
  
  # Save results of long/short leg trade file
  # File dir of trade data, using new direction
  appendCol(data = longTrade, col2append = "matchedLong", file2write = paste0(regTDDir, longRIC, "_", spreadDate, ".csv.gz"))
  appendCol(data = shortTrade, col2append = "matchedShort", file2write = paste0(regTDDir, shortRIC, "_", spreadDate, ".csv.gz"))
}