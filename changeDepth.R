# Function to calculate depth change for a given price
chgDepth <- function(Depth0){
  
  # Index of bid prices and sizes
  BidPrice <- grep("BidPrice",colnames(Depth0), value = TRUE)
  BidSize <- grep("BidSize",colnames(Depth0), value = TRUE)
  
  # Find the bid size of the next depth with the same bid price
  x <- sapply(1:nrow(Depth0), function(x) Depth0[x+1, get(BidSize)][match(Depth0$L1.BidPrice[x],Depth0[x+1,..BidPrice])])
  
  # If the price disappear, set the chg of size to the order size
  # Otherwise, take the difference
  chgBid <- Depth0$L1.BidSize-ifelse(is.na(x), 0, x)
  
  # Index of ask prices and sizes
  AskPrice <- grep("AskPrice",colnames(Depth0), value = TRUE)
  AskSize <- grep("AskSize",colnames(Depth0), value = TRUE)
  
  # Find the ind of the same ask price in the following order
  y <- sapply(1:nrow(Depth0), function(x) Depth0[x+1, get(AskSize)][match(Depth0$L1.AskPrice[x], Depth0[x+1,..AskPrice])])
  
  # If the price disappear, set the chg of size to the order size
  # Otherwise, take the difference
  chgAsk <- Depth0$L1.AskSize-ifelse(is.na(y), 0, y)

  # return(data.frame("chgBid"=as.numeric(chgBid),"chgAsk"=as.numeric(chgAsk)))
  return(list("chgBid"=chgBid,"chgAsk"=chgAsk))
}

# Below is for testing

# comDepth <- comDepth[, c("chgBid", "chgAsk"):= chgDepth(comDepth[.I,],comDepth[.I+1,]), by=.(X.RIC, Date)]

# comDepth[2,c("L1.BidPrice","L2.BidPrice","L3.BidPrice","L4.BidPrice","L5.BidPrice")]
# comDepth[2,c("L1.BidSize","L2.BidSize","L3.BidSize","L4.BidSize","L5.BidSize")]
# 
# comDepth[3,c("L1.BidPrice","L2.BidPrice","L3.BidPrice","L4.BidPrice","L5.BidPrice")]
# comDepth[3,c("L1.BidSize","L2.BidSize","L3.BidSize","L4.BidSize","L5.BidSize")]
# 
# comDepth[2,c("L1.AskPrice","L2.AskPrice","L3.AskPrice","L4.AskPrice","L5.AskPrice")]
# comDepth[2,c("L1.AskSize","L2.AskSize","L3.AskSize","L4.AskSize","L5.AskSize")]
# 
# comDepth[3,c("L1.AskPrice","L2.AskPrice","L3.AskPrice","L4.AskPrice","L5.AskPrice")]
# comDepth[3,c("L1.AskSize","L2.AskSize","L3.AskSize","L4.AskSize","L5.AskSize")]
# 
# comDepth <- loadTrades("F:/FMM/longperiod/FuturesDepth/OMXS30V4_2014-10-07.csv.gz")