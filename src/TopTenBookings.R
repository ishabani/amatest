print("the number of lines in bookings file is %s", system("wc -l bookings.csv |cut -d \' \' -f1"))
print("the number of lines in searches file is %s", system("wc -l searches.csv |cut -d \' \' -f1"))
bookings<-read.csv("bookings.csv",header=T, sep="^", fill=T)
summary(bookings)
names(bookings)
library(data.table)
DT<-data.table(bookings)
Tdf<-as.data.frame(DT[,j=list(sumpax=sum(pax)), by=list(arr_port)])
sor <-Tdf[order(-Tdf$sumpax),]
names(sor)
topten<-sor[1:10,1:2]
topten


