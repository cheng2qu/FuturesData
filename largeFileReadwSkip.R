# Load compiled trading and depth data (2-4GB)

require(data.table)

# Add 7.zip into the PATH for uncompressing
Sys.setenv(PATH = paste(Sys.getenv("PATH"),
                        "C:\\Program Files\\7-Zip",
                        sep = ";"))

# Obtain row index of Quotes data
# sapply(scan(gzfile("Z:/Documents/Futures rollover/OMXS30FuturesTrades.csv.gz"), sep = "\n", n = 10, skip = 1, what = character()), grepl, pattern="Quote")

# Try uncompress the data and load by fread
# NOTE: Can only uncompress the zipped file to the dir
# fread("7z e OMXS30SpreadTQ.csv.gz")

largeFileRead <- function(largeFile) {
  colNames <- colnames(fread(input = paste0("7z x -so ", largeFile), header = TRUE, nrows = 10, check.names=T))
  
  # Through N lines
  N <- 100000
  
  # Set index for loop
  indexRep = 0
  
  # Start the loop until the subsequent chunk contains fewer rows
  repeat{
    # Find index of rows to skip
    trade.sample <- fread(input = paste0("7z x -so ", largeFile),
                          nrows=N,
                          skip = N*indexRep,
                          stringsAsFactors = FALSE,
                          header = indexRep == 0,
                          col.names = colNames)
    
    NR <- nrow(trade.sample)
    # Drop quotes and trades with non-positive price
    if (grepl("Trade", largeFile)) {
       trade.sample <- trade.sample[Type!="Quote" & Price>0,]
    }
   
    # Fill trading date
    trade.sample <- trade.sample[, Date:=substr(Date.Time,1,10)]

    # Write the subsets of data, in .txt/.csv format
    trade.sample[, fwrite(x = trade.sample[.I],
                          file=sub(".csv.gz",paste0("/", X.RIC[1], "_", Date[1], ".csv"), largeFile),
                          append = TRUE,
                          row.names = FALSE,
                          col.names = !file.exists(sub(".csv.gz",paste0("/", X.RIC[1], "_", Date[1], ".csv"), largeFile)),
                          sep = ","),
                 by = .(X.RIC, Date)]
    
    # Test the number of rows for each RIC on each trading day
    # trade.sample[, nrow(trade.sample[.I]), by = .(X.RIC, Date)]
    
    # Compress csv files to gz files
    # NOTE: gzfile cannot append rows
    # trade.sample[, system(paste0("gzip ", sub(".csv.gz",paste0("/", X.RIC, "_", Date, ".csv"), largeFile))),
    #              by = .(X.RIC, Date)]
    
    # print(paste(trade.sample[1,"X.RIC"],substr(trade.sample[1,Date.Time],1,10)))
    indexRep <- indexRep + 1
    
    if (NR < N) {
      break
    }
  }
}
# 
# # New folders for split files are needed
# # Apply function to trades and depth data
# tem <- largeFileRead("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth.csv.gz")
# tem <- largeFileRead("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ.csv.gz")
# 
# # Compress csv files to gz files
# setwd("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth/")
# sapply(list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularDepth/"), function(x) system(paste0("gzip ",x), wait = FALSE))
# 
# setwd("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/")
# sapply(list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/"), function(x) system(paste0("gzip ",x), wait = FALSE))

# Files cannot be compressed
# Need to delect gz file with 0-size and compress the csv files again
# sapply(list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/")[grepl(".csv$", list.files("E:/FMM/OMXS30Futures/TRTHv2/Table3_subsample/RegularTAQ/"))],
#        function(x) system(paste0("gzip ",x), wait = FALSE))

# Try to read the split gzfile
# read.csv(gzfile("Z:/Documents/Futures rollover/OMXS30FuturesTrades/OMXS30F4_2013-11-21.csv.gz"))
