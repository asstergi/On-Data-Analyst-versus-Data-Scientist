library(wordcloud)

data = read.csv("PMIs for data_analyst & data_scientist.csv")

higher = data[,2]>data[,3]
colours = rep('#0878FD', length(higher))
colours[higher] = '#FD7F08'

mx <- apply(data[,2:3],2,max)
mn <- apply(data[,2:3],2,min)
par(bg = '#FEFEFE')
textplot(data[,2],data[,3],data[,1],
         show.lines=F, cex = 0.6, col=colours,
         xlim=c(mn[1],mx[1]),ylim=c(mn[2],mx[2]),
         tstep=.05, rstep=.05)