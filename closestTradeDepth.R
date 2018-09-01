# Function closestDepth 
# and Function closestTrade

require(data.table)

# function to fix trading session----
ifDiffSession <- function(tradeSec,depthSec, tradeDate){
  # Half trading day according to the trading calendar
  halfTradeDay <- read.table("Half-trading days.txt", header = TRUE)
  halfTradeDay$Date <- as.character(halfTradeDay$Date)
  if (tradeDate %in% halfTradeDay$Date) {  
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 12*3600+55*60, 13*3600, 13*3600+30*60)
    return(findInterval(tradeSec, hourInterval)!= findInterval(depthSec, hourInterval))
  } else {
    hourInterval <- c(8*3600+30*60, 8*3600+55*60, 9*3600, 17*3600+25*60, 17*3600+30*60, 18*3600)
    return(findInterval(tradeSec, hourInterval)!= findInterval(depthSec, hourInterval))
  }
}

# Function searching for closest matched depth --------
# from all the depth records according to trade second
closestDepth <- function(tradeSecond, depthSecond, tradeDate) {

  ind <- NA
  if (tradeSecond>=min(depthSecond)) {
    ind <- which.max((depthSecond-tradeSecond)[(depthSecond-tradeSecond)<=0]) # Find the index of the closest quote before
    
    if (ifDiffSession(tradeSecond, depthSecond[ind], tradeDate)) {
      ind <- NA
    }
  }
  
  if (length(ind)>1) {
    ind <- ind[1]
    }
  return(ind)
}

# Function searching for closest matched trade -----------------------
closestTrade <- function(tradeSecond, trade2Second, tradeDate) {
  
    ind <- which.min(abs(trade2Second-tradeSecond)) # Find the index of the closest quote around: before or after
    
    if (ifDiffSession(tradeSecond, trade2Second[ind],tradeDate)) {
      ind <- NA
    }
    
    if (length(ind)>1) ind <- ind[1]
  return(ind)
}