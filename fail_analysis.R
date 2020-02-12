################# Fail Analysis Regression #######################
##install.packages("readr")
##install.packages("dplyr")
library(readr)
library(dplyr)
fail_1 <- read_csv("~/brunoa/Fail_analysis/csv/fails_1_BRA.csv")
fail_2 <- read_csv("~/brunoa/Fail_analysis/csv/fails_2_BRA.csv")
fail_3 <- read_csv("~/brunoa/Fail_analysis/csv/fails_3_BRA.csv")

##################### Limpieza de variables ##############
fail_1 <- data.frame(fail_1)
fail_2 <- data.frame(fail_2)
fail_3 <- data.frame(fail_3)


fail <- inner_join(fail_1, fail_2, by = c("city_name", "week"))
fail <- inner_join(fail, fail_3, by = c("city_name", "week"))
fail$late <- as.numeric(fail$late)
fail$pinwheel <- as.numeric(fail$pinwheel)
fail$address_change <- as.numeric(fail$address_change)
fail[is.na(fail)] <- 0
fail <- select(fail,-matches("country_name"))
fail <- select(fail,-matches("city_id"))

##Remove latest (incomplete) week
fail <- fail[!(fail$week=="2019-07-29"),]

##Batched trips
fail$batched <- (fail$batched_trips/fail$orders)

##Remove outliers
quantile(fail$failed, c(.25, .50, .98))
quantile(fail$failed_cancels, c(.25, .50, .98))
fail <- fail[!(fail$failed > 0.018),]
fail <- fail[!(fail$failed_cancels > 0.033),]
summary(fail)



######################## Paquetes #######################################
#install.packages("FactoMineR")
#install.packages("factoextra")
#install.packages("gplots")
#install.packages("plm")
#install.packages("foreign")
library(factoextra)
library(FactoMineR)
library(gplots)
library(foreign)
library(plm)
library(car)

########################## Normalizar las variables ########################################
Seg_scale <- scale(fail)


scatterplot(failed_cancels~week|city_name, boxplots=FALSE, smooth=TRUE, reg.line=FALSE, data=fail)
plotmeans(failed_cancels ~ city_name, main="Heterogeineity across countries", data=fail)
plotmeans(failed_cancels ~ week, main="Heterogeineity across years", data=fail)

plotmeans(failed_cancels ~ More_Than_6_Months, main="Fail rate across experienced dp", data=fail)

plot(fail$failed_cancels, fail$cash, main = "Fails vs Cash",
     xlab = "Cash penetration", ylab = "Fail Rate",
     pch = 19, frame = FALSE)
abline(lm(failed_cancels ~ cash, data = fail), col = "blue")

plot(fail$failed_cancels, fail$More_Than_6_Months, main = "Fails vs Exp",
     xlab = "DP +6 months", ylab = "Fail Rate",
     pch = 19, frame = FALSE)
abline(lm(failed_cancels ~ cash, data = fail), col = "blue")

########################## Regresiones ########################################
## Poled OLS
pool = (plm(failed_cancels ~ xd + cash + card + pinwheel + address_change + late + batched_trips + avg_weekly_earnings_usd
            + Less_Than_1_Month + More_Than_6_Months + s_d_ratio_wo_p2pd
            , data=fail
            , index=c('city_name','week')
            , model='pooling'))
summary(pool)

## Fixed Panel
fixed = (plm(failed_cancels ~ xd + cash + batched + dp_rating_under_90 + motorbike_orders + logistics_orders
             + avg_weekly_earnings_usd + Less_Than_1_Month + More_Than_6_Months + s_d_ratio_wo_p2pd
            , data=fail
            , index=c('city_name','week')
            , model='within'))
summary(fixed)

car_orders
pinwheel
late
address_change

## Random Panel
random = (plm(failed_cancels ~ xd + cash + card + pinwheel + address_change + late + batched_trips + avg_weekly_earnings_usd
             + Less_Than_1_Month + More_Than_6_Months + s_d_ratio_wo_p2pd
             , data=fail
             , index=c('city_name','week')
             , model='random'))
summary(random)

## Evaluate Fixed vs Random
## null hypothesis is that the preferred model is random effects vs. the alternative the fixed effects, p < 0.05 then use fixed
phtest(random, fixed)

