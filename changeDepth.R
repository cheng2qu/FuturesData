# Function to calculate depth change for a given price
chgDepth <- function(Depth0, Depth1){
  
  # Index of bid prices and sizes
  BidPrice <- grep("BidPrice",colnames(Depth0), value = TRUE)

  # Index of ask prices and sizes
  AskPrice <- grep("AskPrice",colnames(Depth0), value = TRUE)

  # Find the column with the same bid price in the following order
  BidInd1 <- match(Depth0$L1.BidPrice, Depth1[,..BidPrice])
  
  # If the price disappear, set the chg of size to the order size
  # Otherwise, take the difference
  if (!is.na(BidInd1)) {
    BidInd1 <- paste0("L", BidInd1, ".BidSize")
    chgBid <- Depth0$L1.BidSize-Depth1[,..BidInd1]
  } else {
    chgBid <- Depth0$L1.BidSize
  }
  
  # Find the ind of the same ask price in the following order
  AskInd1 <- match(Depth0$L1.AskPrice, Depth1[,..AskPrice])
  
  # If the price disappear, set the chg of size to the order size
  # Otherwise, take the difference
  if (!is.na(AskInd1)) {
    AskInd1 <- paste0("L", AskInd1, ".AskSize")
    chgAsk <- Depth0$L1.AskSize-Depth1[,..AskInd1]
  } else {
    chgAsk <- Depth0$L1.AskSize
  }
  return(c("chgBid"=as.numeric(chgBid),"chgAsk"=as.numeric(chgAsk)))
  
}

# Below is for testing

# comDepth <- comDepth[, c("chgBid", "chgAsk"):= chgDepth(comDepth[.I,],comDepth[.I+1,]), by=.(X.RIC, Date)]
# Depth0 <- comDepth[1,]
# Depth1 <- comDepth[2,]

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
# comDepth <- loadTrades("E:/FMM/OMXS30Futures/TRTHv2/longPeriod/OMXS30FuturesL10Depth/OMXS30F2_2011-03-28.csv.gz")