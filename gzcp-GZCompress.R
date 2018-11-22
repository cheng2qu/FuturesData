require(data.table)
library(R.utils)

Sys.setenv(PATH = paste(Sys.getenv("PATH"),
                        "C:\\Program Files\\7-Zip",
                        sep = ";"))

# Function write and compress data -----------------------
# Appending rows
gzcp0 <- function(fileDir, data){
  
  if (file.exists(fileDir)) {
    x <- fread(input = paste0("7z x -so ", fileDir), header = TRUE, check.names=T) # Load existing data
    
    fwrite(x, file = sub(".gz","",fileDir), append = FALSE)
    fwrite(data, file = sub(".gz","",fileDir), append = TRUE)
    gzip(sub(".gz","",fileDir), destname=fileDir, overwrite = TRUE)
  } else {
    fwrite(data, file = sub(".gz","",fileDir), append = TRUE)
    gzip(sub(".gz","",fileDir), destname=fileDir, overwrite = TRUE)
  }
}

# Function to append column-----
appendCol <- function(data, col2append, file2write){
  
  if (!file.exists(file2write)){
    # If file doesn't exist, only save data
    gzcp0(fileDir = file2write, data = data)
  } else {
    # If file exist, read file
    dataOld <- fread(input = paste0("7z x -so ", file2write), header = TRUE, check.names=T)
    if(!col2append %in% colnames(dataOld)) {
      # If the column doesn't exist, attach column
      dataNew <- cbind(dataOld,data[,..col2append])
      # Save new file
      fwrite(dataNew, file = sub(".gz","",file2write), append = FALSE)
      gzip(sub(".gz","",file2write), destname=file2write, overwrite = TRUE)
    }
  }
}