### Modelo de doble diferencia para Clientes Piloto ###

#install.packages("readr")
#install.packages("plm")
#install.packages("foreign")
#install.packages("tidyverse")
#install.packages("haven")
#install.packages("skimr")
#install.packages("CausalImpact") -> Not Working on this session

library(readr)
library(foreign)
library(plm)
library(tidyverse)
library(haven)
library(skimr)
# library(CausalImpact)

### Load dataset ###                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   
panel_dta <- read_dta(file = "~/brunoa/panel_data/data/Panel101.dta")

### Review dataset ### Both methods do basically the same, skim is cleaner
summary(panel_dta)
skim(panel_dta)

##################### Variable IDs ##############

panel_dta$time = ifelse(panel_dta$year >= 1994, 1, 0) # Id for Pre and Post Treatment

panel_dta$treated = ifelse(panel_dta$country == 5 |
                             panel_dta$country == 6 |
                             panel_dta$country == 7, 1, 0) # Id for Control and Treatment Group

##################### Plot Trends to check Parallel Assumptions ##############
panel_dta$treat <- as.factor(panel_dta$treated)

ggplot(panel_dta, aes(year, y, color = treat)) +
  stat_summary(geom = 'line') +
  geom_vline(xintercept = 1994) +
  theme_minimal()

####################### OLS Regression ##############
# Linear model, y as function of x1, x2, x3 and Interaction
ols <- lm(y ~ x1 + x2 + x3 + (time*treated), data=panel_dta)
summary(ols)

################################################### Pooled Panel ##############
# Linear Panel model (Pooled), y as function of x1, x2, x3 and Interaction
summary(plm(y ~ x1 + x2 + x3 + (time*treated),
            data=panel_dta,
            index=c('country','year'),
            model='pooling'
))
# Pooling does not allow for intercept or slope

## Additional Causal Impact analysis -> not working on this session
pre.year <- as.Date(c('1990-01-01', '1993-01-01'))
post.year <- as.Date(c('1994-01-01', '1999-01-01'))
impact <- CausalImpact(panel_dta, pre.period, post.period)
plot(impact)
