require(data.table)
require(ff)

Sys.setenv(PATH = paste(Sys.getenv("PATH"),
                        "C:\\Program Files\\7-Zip",
                        sep = ";"))

# Load R file to clean compiled data for reg trades and depth
source("largeFileReadwSkip.R")
# Load functions for comTDMatch, regTDMatch, and srTDMatch
source("TDmatch.R")

# 0.Apply function to split regular trades and depth data-----
# New folders for split files are needed
tem <- largeFileRead("//tsclient/E/FMM/OMXS30Futures/TRTHv2/longPeriod/20180813_Futures_Depth.csv.gz")
tem <- largeFileRead("//tsclient/E/FMM/OMXS30Futures/TRTHv2/longPeriod/20180813_Futures_TAQ.csv.gz")

# Compress csv files to gz files
# setwd("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth/")
# sapply(list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth/"), function(x) system(paste0("gzip ",x), wait = FALSE))
# 
# setwd("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/")
# sapply(list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/"), function(x) system(paste0("gzip ",x), wait = FALSE))

# 1.Match spreads trades and depth-------------
# Load data of spreads trades, which are complied in a single file

comTrade <- loadTrades("E:/FMM/OMXS30Futures/TRTHv2/longPeriod/20180813_Spread_TAQ.csv.gz")
comDepth <- loadTrades("E:/FMM/OMXS30Futures/TRTHv2/longPeriod/20180813_Spread_Depth.csv.gz")

# Below is testing
# comFullTrade <- comTrade
# comFullDepth <- comDepth

# comTrade <- comFullTrade[grepl("^OMXS30M6-N6", comFullTrade$X.RIC) & (grepl("^2016-05-23", comFullTrade$Date)),]
# comDepth <- comFullDepth[grepl("^OMXS30G2-H2", comFullDepth$X.RIC) & (grepl("^2012-02-17", comFullDepth$Date)),]

# comTrade <- comFullTrade
# comDepth <- comFullDepth

# Apply matching function
comTrade[, tdComMatch(comTrade = comTrade[.I], 
                      comDepth = comDepth[which(comDepth$X.RIC == comTrade$X.RIC[.I[1]] & comDepth$Date==comTrade$Date[.I[1]]),],
                      output = "E:/FMM/OMXS30Futures/TRTHv2/longPeriod/comTrade_5depth.csv"),
         by = .(X.RIC, Date)]
system("gzip E:/FMM/OMXS30Futures/TRTHv2/longPeriod/comTrade_5depth.csv", wait = FALSE)

# Load comTradeDepth
comTrade <- fread(input = "7z x -so G:/FMM/longperiod/comTrade_5depth.csv.gz", header = TRUE, check.names=T)

# Drop matched spread contract from the comTrade data
# fileDone <- list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/comRegTD/", pattern=".csv.gz", all.files=FALSE,full.names=FALSE)
# fileDone <- data.table(X.RIC = tstrsplit(fileDone,split = "_")[[1]], Date = tstrsplit(fileDone,split = "_")[[2]])
# comTrade <- comTrade[!paste0(X.RIC, Date) %in% paste0(fileDone$X.RIC, fileDone$Date),]

# 2.Match spreads trades and regular trades and depth------------
# Traing data, both legs, are loaded in the function
# Apply srTradeMatch function defined in TDmatch.R
# Apply function by RIC and Date of the spreads trade
# The function will search corresponding long and short legs for spreads trade
# comTrade[, grep("^long",colnames(comTrade))] <- NULL
# comTrade[, grep("^short",colnames(comTrade))] <- NULL
# comTrade[, c("priceLong", "priceShort")] <- NULL

comTrade[, c("priceLong", "priceShort")] <- numeric(0)

comTrade[, srTDMatch(comTrade[.I],
                     regTradeDir = "E:/FMM/OMXS30Futures/TRTHv2/subsample201605/FuturesTAQ/",
                     regDepthDir = "E:/FMM/OMXS30Futures/TRTHv2/subsample201605/FuturesDepth/"),
         by = .(X.RIC, Date)]

comTrade[, srTDMatch(comTrade[.I],
                     regTradeDir = "G:/FMM/subsample201605/FuturesTAQ/",
                     regDepthDir = "G:/FMM/subsample201605/FuturesDepth/"),
         by = .(X.RIC, Date)]

# Test without hard drive
comTrade[, srTDMatch(comTrade[.I],
                     regTradeDir = "Z:/Documents/sample_TAQ/",
                     regDepthDir = "Z:/Documents/sample_Depth/"),
         by = .(X.RIC, Date)]

# Drop matched spread contract from the comTrade data
fileDone <- list.files("G:/FMM/subsample201605/comRegTD/", pattern=".csv.gz", all.files=FALSE,full.names=FALSE)
fileDone <- data.table(X.RIC = tstrsplit(fileDone,split = "_")[[1]], Date = tstrsplit(fileDone,split = "_")[[2]])
comTrade <- comTrade[paste0(X.RIC, Date) %in% paste0(fileDone$X.RIC, fileDone$Date),]

# comTrade <- comFullTrade[!paste0(X.RIC, Date) %in% paste0(fileDone$X.RIC, fileDone$Date),]
# table(comTrade$X.RIC, comTrade$Date)

# If all spreads trades are matched with correct quotes
# which(! comTrade$Price %in% c(comTrade$bestAsk,comTrade$bestBid))

# If all spreads trades are matched with correct regular trades
# which(round(comTrade$priceLong-comTrade$priceShort-comTrade$Price,2)!=0)
# which(is.na(comTrade$priceLong))
# If both legs of regular trades are matched
# setdiff(longTrade$matchedLong,shortTrade$matchedShort)
# setdiff(shortTrade$matchedShort,longTrade$matchedLong)

# Checking match result

ifMatch <- function(file) {
  
  # Load data
  comTrade <- fread(input = paste0("7z x -so ", file), header = TRUE, check.names=T)
  # If spread price matches with spread quote
  comTrade <- comTrade[, ifMatchComPrice := (Price %in% c(bestBid,bestAsk))]
  # If reg price matches with spread price
  comTrade <- comTrade[, ifMatchComReg := (round(priceLong-priceShort-Price,2)==0)]
  # Save and append matching result
  matchResult <- paste(comTrade$X.RIC[1], 
                       comTrade$Date[1],
                       1-round(nrow(comTrade[ifMatchComPrice=="TRUE"])/nrow(comTrade),2),
                       1-round(nrow(comTrade[ifMatchComReg=="TRUE"])/nrow(comTrade),2),
                       sep = ", "
  )
  cat(matchResult,file="matchResult.txt",sep="\n", append = TRUE)
  
}

fileDone <- list.files("Z:/Documents/comRegTD/", pattern=".csv.gz", all.files=FALSE, full.names=FALSE)
fileDone <- fileDone[grep("OMXS30",fileDone)]
tem <- sapply(paste0("Z:/Documents/Futures rollover/Results20180604/",fileDone), ifMatch)