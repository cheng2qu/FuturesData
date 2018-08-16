require(data.table)
require(ff)
# Function write and compress data -----------------------
# Appending rows
gzcp <- function(fileDir, Cont) {
  zzfil <- fileDir
  if (file.exists(zzfil)) {
    x <- read.table(gzfile(fileDir), sep = ",",header = TRUE) # Load existing data
  } else {
    x <- Cont[0,]
  }
  
  zz <- gzfile(zzfil, "w")  # compressed file
  write.table(x, file = zz, sep = ",", append = TRUE, col.names = TRUE, row.names = FALSE)
  write.table(Cont, file = zz, sep = ",", append = TRUE, col.names = FALSE, row.names = FALSE)
  close(zz)
}

# Function write and compress data -----------------------
# Appending columns
gzcp0 <- function(fileDir, Cont) {
  zzfil <- fileDir
  if (file.exists(zzfil)) {
    x <- read.table(gzfile(fileDir), sep = ",",header = TRUE) # Load existing data
    if (length(setdiff(colnames(Cont), colnames(x)))>0) {
      x[[setdiff(colnames(Cont), colnames(x))]] <- Cont[[setdiff(colnames(Cont), colnames(x))]]
      Cont <- x
    }
  }
  
  zz <- gzfile(zzfil, "w")  # compressed file
  write.table(Cont, file = zz, sep = ",", append = FALSE, col.names = TRUE, row.names = FALSE)
  close(zz)
}
