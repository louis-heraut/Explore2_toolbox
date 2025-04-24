# Ex2D toolbox [<img src="resources/LogoExplore2.png" align="right" width=160 alt=""/>](https://professionnels.ofb.fr/fr/node/1244)

<!-- badges: start -->
[![Lifecycle: maturing](https://img.shields.io/badge/lifecycle-maturing-blue.svg)](https://lifecycle.r-lib.org/articles/stages.html)
![](https://img.shields.io/github/last-commit/super-lou/Explore2_toolbox)
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-2.1-4baaaa.svg)](code_of_conduct.md) 
<!-- badges: end -->

**Ex2D toolbox** is a R toolbox based on [EXstat](https://github.com/super-lou/EXstat) and [CARD](https://github.com/super-lou/CARD) packages which provide a simple way of interacting with all these codes to carry out diagnostic of hydrological models used in [Explore2](https://professionnels.ofb.fr/fr/node/1244).

This project was carried out for National Research Institute for Agriculture, Food and the Environment (Institut National de Recherche pour l’Agriculture, l’Alimentation et l’Environnement, [INRAE](https://agriculture.gouv.fr/inrae-linstitut-national-de-recherche-pour-lagriculture-lalimentation-et-lenvironnement) in french).


## Installation
For latest development version
``` 
git clone https://github.com/super-lou/Explore2_toolbox.git
```


## Help
You can find different help script in this the [help](https://github.com/super-lou/Explore2_toolbox/tree/main/help) directory.

### DRIAS NetCDF exportation
In the [help](https://github.com/super-lou/Explore2_toolbox/tree/main/help) directory, you will have the [DRIAS_export](https://github.com/super-lou/Explore2_toolbox/tree/main/help/DRIAS_export) directory which includes 2 subdirectories, one for [1D data exportation](https://github.com/super-lou/Explore2_toolbox/tree/main/help/DRIAS_export/DRIAS_export_1D) and the other for [2D data exportation](https://github.com/super-lou/Explore2_toolbox/tree/main/help/DRIAS_export/DRIAS_export_2D).

You can make your choice between [1D](https://github.com/super-lou/Explore2_toolbox/tree/main/help/DRIAS_export/DRIAS_export_1D) and [2D](https://github.com/super-lou/Explore2_toolbox/tree/main/help/DRIAS_export/DRIAS_export_2D) DRIAS NetCDF exportation and find a README in each subdirectory.

### Formatted Rdata exportation
In [Rdata_export](https://github.com/super-lou/Explore2_toolbox/tree/main/help/Rdata_export), there is an example of what formatted data for Explore2 looks like.


## Main execution
In the principal directory of this toolbox you will find the [main.R](https://github.com/super-lou/Explore2_toolbox/tree/main/main.R) script which is dedicated to fast global execution of the Explore2 diagnostic by using [MKstat](https://github.com/super-lou/MKstat), [ashes](https://github.com/super-lou/ashes) and [dataSheep](https://github.com/super-lou/dataSheep) R package.

*in construction*


## FAQ
📬 — **I would like an upgrade / I have a question / Need to reach me**  
Feel free to [open an issue](https://github.com/super-lou/Explore2_toolbox/issues) ! I’m actively maintaining this project, so I’ll do my best to respond quickly.  
I’m also reachable on my institutional INRAE [email](mailto:louis.heraut@inrae.fr?subject=%5BExplore2_toolbox%5D) for more in-depth discussions.

🛠️ — **I found a bug**  
- *Good Solution* : Search the existing issue list, and if no one has reported it, create a new issue !  
- *Better Solution* : Along with the issue submission, provide a minimal reproducible code sample.  
- *Best Solution* : Fix the issue and submit a pull request. This is the fastest way to get a bug fixed.

🚀 — **Want to contribute ?**  
If you don't know where to start, [open an issue](https://github.com/super-lou/Explore2_toolbox/issues).

If you want to try by yourself, why not start by also [opening an issue](https://github.com/super-lou/Explore2_toolbox/issues) to let me know you're working on something ? Then:

- Fork this repository  
- Clone your fork locally and make changes (or even better, create a new branch for your modifications)
- Push to your fork and verify everything works as expected
- Open a Pull Request on GitHub and describe what you did and why
- Wait for review
- For future development, keep your fork updated using the GitHub “Sync fork” functionality or by pulling changes from the original repo (or even via remote upstream if you're comfortable with Git). Otherwise, feel free to delete your fork to keep things tidy ! 

If we’re connected through work, why not reach out via email to see if we can collaborate more closely on this repo by adding you as a collaborator !


## Code of Conduct
Please note that this project is released with a [Contributor Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.
