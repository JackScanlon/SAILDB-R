
<!-- README.md is generated from README.Rmd. Please edit that file -->

<picture>
<source media="(prefers-color-scheme: dark)" srcset="man/assets/sail-darkmode.png">
<img align="right" src="man/assets/sail-lightmode.png" alt="SAIL Databank Logo" height="70" title="SAIL Databank">
</picture>

<br/> <br/>

# SAILR

An unofficial package to interface and interact with [SAIL
Databank](https://saildatabank.com)’s database via R.

<!-- badges: start -->

[![Project Status: Active – The project has reached a stable, usable
state and is being actively
developed.](https://www.repostatus.org/badges/latest/active.svg)](https://www.repostatus.org/#active)
[![License:
MIT](https://img.shields.io/badge/license-MIT-blue)](https://www.tldrlegal.com/license/mit-license)
<!-- badges: end -->

## Getting Started

### Installation

1.  Installing `SAILR` whilst inside SAIL’s TRE Gateway:
    - Running the `installer.R` script found within
      `R:/R_Packages/SAIL_DBI` directly, or by sourcing it:

    ``` r
    # Somewhere in your project
    source('R:/R_Packages/SAIL_DBI/installer.R')
    ```

    - Or; you can install directly using the `install.packages()`
      function:

    ``` r
    # Install from resource (`R:/`) directory somewhere in your project
    install.packages(
      pkgs   = 'R:/R_Packages/SAIL_DBI/SAILR_0.1.0.tar.gz',
      repos  = NULL,
      source = 'source'
    )
    ```
2.  Installing `SAILR` whilst outside the gateway:

> \[!IMPORTANT\]  
> You won’t be able to use this method whilst inside the gateway as
> Github is blocked for security; use the first method if you are
> developing inside the gateway

``` r
# Install devtools if not already installed
install.packages('devtools')

# Install from github
devtools::install_github('JackScanlon/SAILR')
```

### Connecting to SAIL’s DB

Once installed, you may connect to the database as described below:

``` r
# Load the package
library(SAILR)

# Connect to the datase
#
#  NOTE:
#   - You will be automatically connected using your gateway credentials if
#     they have already been stored by `SAILR::Profile`; otherwise you will
#     be prompted to enter your username/password
#
db = Connection$new()

# Check if we're connected (not required)
if (db$connected) {
  print('We are connected!')
}

# Perform some query
db$run("
  SELECT
      'Hello, world!' AS MESSAGE
    FROM SYSIBM.SYSDUMMY1;
")
```

## Example usage

To view detailed examples and their explanations, see:

- If you’re viewing from the documentation website: please see the
  [Reference](reference/index.html) page(s) and the available
  article(s), *e.g.* the [Connection](articles/Connection.html) article
- If you’re viewing via Github: please see the [documentation](./man)
  and [vignettes](./vignettes)
