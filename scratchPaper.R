# Scratch paper
# To load raw data and run the matching function
# Redefine tdRegMatch function for regular legs without spreads
# Load comTradeDepth
comTrade <- fread(input = "7z x -so F:/FMM/longperiod/comTrade_5depth.csv.gz", header = TRUE, check.names=T)

comTrade <- comTrade[grepl("^OMXS30", comTrade$X.RIC) & (grepl("^2011", comTrade$Date)),]
fullTrade <- comTrade
comTrade <- fullTrade[1:30,]



comTrade[, srTDMatch(comTrade[.I],
                     regTradeDir = "F:/FMM/20181205/FuturesTAQ/",
                     regDepthDir = "F:/FMM/20181205/FuturesDepth/"),
         by = .(X.RIC, Date)]

# Drop matched spread contract from the comTrade data
fileDone <- list.files("F:/FMM/20181205/comRegTD/", pattern=".csv.gz", all.files=FALSE,full.names=FALSE)
fileDone <- data.table(X.RIC = tstrsplit(fileDone,split = "_")[[1]], Date = tstrsplit(fileDone,split = "_")[[2]])
comTrade <- comTrade[!paste0(X.RIC, Date) %in% paste0(fileDone$X.RIC, fileDone$Date),]
# table(comTrade$X.RIC, comTrade$Date)

# Redefine tdRegMatch for unprocessed trades
tdRegMatch <- function(fileDir) {
  # The function is modified from tdComMatch
  # So the data is named with prefix com-
  comTrade <- loadTrades(fileDir = fileDir)
  comDepth <- loadTrades(fileDir = sub("FuturesTAQ", "FuturesDepth", fileDir))
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
    if (!is.na(comTrade$L1.BidPrice[i]) | comTrade$Type[i]=="offExchange") next
    
    if(i==1){
      # If start from pre-trade or normal session
      tradeStart <- c(8*3600+30*60, 8*3600+55*60)[1+(comTrade$Second[i]>=8*3600+55*60)]
      
      # Matching with exact price and size
      indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                            comDepth$Date == comTrade$Date[i]&
                            comDepth$Second <= comTrade$Second[i]&
                            (comDepth$Second - comTrade$Second[i])>=(tradeStart - comTrade$Second[i])&
                            ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])|
                               (comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$L1.BidSize==comTrade$Volume[i]) |
                               (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$L1.AskSize==comTrade$Volume[i])))
      
      if(length(indexDepth)==0){
        # Matching with price interval and ignore the size
        indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
                              comDepth$Date == comTrade$Date[i]&
                              comDepth$Second <= comTrade$Second[i]&
                              (comDepth$Second - comTrade$Second[i])>=(tradeStart - comTrade$Second[i])&
                              comDepth$L1.BidPrice<=comTrade$Price[i] & comDepth$L1.AskPrice>=comTrade$Price[i])
      }
      
      if (length(indexDepth)>=1) {
        # Assign the values if find matches
        indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
        comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
        next
      } else {
        
        # If no match for the first trade, try with the closest match
        indexDepth <- which.max((comDepth$Second-comTrade$Second[i])[(comDepth$Second-comTrade$Second[i])<=0])
        if (length(indexDepth)==0){
          next
        }
        # Exclude matching cross trading session
        if(ifDiffSession(comTrade[i,Second],comDepth[indexDepth,Second],comTrade[i,Date])) {
          comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- NA
          comTrade[i][,(L10Depths)] <- NA
          next
        } else {
          next
        }
      }
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
    if (length(indexDepth)>=1) {
      # Assign the values if find matches
      indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
      comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
      comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
      next
    }
    # 
    # # 2. Match to next depth if no trades happen in the interval====
    # if (length(indexDepth)==0) {
    #   indexDepth <- which(comDepth$X.RIC == comTrade$X.RIC[i]& 
    #                         comDepth$Date == comTrade$Date[i]&
    #                         comDepth$Second > comTrade$Second[i]&
    #                         (comDepth$Second - comTrade$Second[i])<(comTrade$Second[i+1] - comDepth$Second)&
    #                         ((comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$chgBid==comTrade$Volume[i]) |
    #                            (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$chgAsk==comTrade$Volume[i])|
    #                            (comDepth$L1.BidPrice==comTrade$Price[i] & comDepth$L1.BidSize==comTrade$Volume[i])|
    #                            (comDepth$L1.AskPrice==comTrade$Price[i] & comDepth$L1.AskSize==comTrade$Volume[i])))
    #   
    #   indexDepthNext <- which(comDepth$X.RIC == comTrade$X.RIC[i+1]& 
    #                             comDepth$Date == comTrade$Date[i+1]&
    #                             comDepth$Second <= comTrade$Second[i+1]&
    #                             (comDepth$Second - comTrade$Second[i+1])>=(comTrade$Second[i] - comTrade$Second[i+1])&
    #                             ((comDepth$L1.BidPrice==comTrade$Price[i+1] & comDepth$chgBid==comTrade$Volume[i+1]) |
    #                                (comDepth$L1.AskPrice==comTrade$Price[i+1] & comDepth$chgAsk==comTrade$Volume[i+1])|
    #                                (comDepth$L1.BidPrice==comTrade$Price[i+1] & comDepth$L1.BidSize==comTrade$Volume[i+1]) |
    #                                (comDepth$L1.AskPrice==comTrade$Price[i+1] & comDepth$L1.AskSize==comTrade$Volume[i+1])))
    #   indexDepth <- indexDepth[!indexDepth %in% indexDepthNext]
    # }
    
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
      if (length(indexDepth)>=1) {
        # Assign the values if find matches
        indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
        comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
        comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
        next
      }
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
        }
        # Consider same depth
        if(!is.na(comTrade[i-1, L1.BidPrice]) & !is.na(comTrade[i-1, L1.AskPrice])) {
          if(comTrade[i-1, L1.BidPrice]<=comTrade$Price[i] &  comTrade[i-1, L1.AskPrice]>=comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L1.AskPrice","L1.BidSize","chgAsk")]
            comTrade[i][,(L10Depths)] <- comTrade[i-1, ..L10Depths]
            next
          }
        }
        # If there is no depth between two trades, try to find match cross depths
        # Through the bid side, with exact price
        if(!is.na(comTrade[i-1, L2.BidPrice])) {
          if(comTrade[i-1, L2.BidPrice]==comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L2.BidPrice","L1.AskPrice","L2.BidSize","chgAsk")]
            comTrade[i][,(L10Depths)] <- ttDepth("B", comTrade[i-1, ..L10Depths])
            next
          }
        }
        # Through the ask side, with exact price
        if(!is.na(comTrade[i-1, L2.AskPrice])) {
          if(comTrade[i-1,L2.AskPrice]==comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
            comTrade[i][,(L10Depths)] <- ttDepth("A", comTrade[i-1, ..L10Depths])
            next
          }
        }
        # Through the bid side, with price bounds
        if(!is.na(comTrade[i-1, L2.BidPrice]) & !is.na(comTrade[i-1, L1.AskPrice])) {
          if(comTrade[i-1, L2.BidPrice]<=comTrade$Price[i] & comTrade[i-1, L1.AskPrice]>=comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L2.BidPrice","L1.AskPrice","L2.BidSize","chgAsk")]
            comTrade[i][,(L10Depths)] <-  ttDepth("B", comTrade[i-1, ..L10Depths])
            next
          }
        }
        # Through the bid side, with price bounds
        if(!is.na(comTrade[i-1, L1.BidPrice]) & !is.na(comTrade[i-1, L2.AskPrice])) {
          if(comTrade[i-1, L1.BidPrice]<=comTrade$Price[i] & comTrade[i-1,L2.AskPrice]>=comTrade$Price[i]) {
            comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comTrade[i-1, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
            comTrade[i][,(L10Depths)] <- ttDepth("A", comTrade[i-1, ..L10Depths])
            next
          }
        }
      } else{
        # If trade through the depth in between
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
              comTrade[i][,(L10Depths)] <-  ttDepth("B", comDepth[indexDepth,])
              next
            }
          }
          
          if(!is.na(comDepth[indexDepth, L1.BidPrice]) & !is.na(comDepth[indexDepth, L2.AskPrice])){
            if(comDepth[indexDepth, L1.BidPrice]<=comTrade$Price[i] & comDepth[indexDepth,L2.AskPrice]>=comTrade$Price[i]) {
              comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L2.AskPrice","chgBid","L2.AskSize")]
              comTrade[i][,(L10Depths)] <- ttDepth("A", comDepth[indexDepth,])
              next
            }
          }
          
        }
      }
    }
    if (length(indexDepth)>=1) {
      # Assign the values if find matches
      indexDepth <- indexDepth[which.min(abs(comDepth$Second[indexDepth]-comTrade$Second[i]))]
      comTrade[i, c("bestBid", "bestAsk", "chgBid", "chgAsk")] <- comDepth[indexDepth, c("L1.BidPrice","L1.AskPrice","chgBid","chgAsk")]
      comTrade[i][,(L10Depths)] <- comDepth[indexDepth, ..L10Depths]
    }
    # If there is still no match, discuss later
  }

  comTrade <- comTrade[Price==bestBid, initiate := "Sell"][Price==bestAsk, initiate := "Buy"]
  
  comTrade[, matchedLong:=0]
  comTrade[, matchedShort:=0]
  gzcp0(fileDir = sub("FuturesTAQ", "RegDepthTAQ", fileDir), data = comTrade)
}
