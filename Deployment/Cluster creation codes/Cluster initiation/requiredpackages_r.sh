http_proxy=http://proxy.ouraes.com:8080
export http_proxy
https_proxy=https://proxy.ouraes.com:8080
export https_proxy


Rscript -e 'install.packages("stringi", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("caret", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("e1071", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("ggplot2", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("stats", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("utils", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("http://cran.r-project.org/src/contrib/Archive/xgboost/xgboost_1.0.0.1.tar.gz", repos=NULL, type="source")'
Rscript -e 'install.packages("qgraph", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("ranger", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("class", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("dummies", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("dplyr", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("tidyverse", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("readr", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("caret", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("reshape", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("http://cran.r-project.org/src/contrib/Archive/caTools/caTools_1.17.tar.gz", repos=NULL,dependencies=TRUE)'
Rscript -e 'install.packages("fastDummies", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("bigrquery", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("readr", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("httpuv", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("DBI", repos="http://cran.us.r-project.org")'
Rscript -e 'install.packages("rlist", repos="http://cran.us.r-project.org")'