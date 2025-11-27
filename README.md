# Toolkit files:
- energy_use_reconciliation.py: Reconcile energy use totals against ESTO balances. Designed so that you only need to create functions which calcaulte energy use from a branch, and adjust the inptus accordingly.The rest of the code doesnt need to be changed if you are using this for a sector other than transport. The transport examples for this are in energy_use_reconciliation_transport.py
- esto_transport_data.py: Functions for reading ESTO transport data files ready for energy use reconciliation.
- MAIN_other_sectors.py: Brief script used to explore use of the LEAP_core and LEAP_excel_io modules for non-transport sectors. Currently only has a stub for energy use reconciliation.
- LEAP_core.py: Core LEAP functions for reading/writing LEAP files, and pushing data into LEAP via COM. Hasnt had much of an attempt to be generalized, so transport-specific code is still present.
- LEAP_excel_io.py: Functions for reading/writing LEAP Excel import/export files. Also has transport-specific code.

# LEAP Transport code

Code to turn transport model spreadsheets into LEAP-ready files. They also check the totals against ESTO balances and can push values straight into LEAP if you have it open. Most of the code is really about mapping the 9th edition transport model structure into LEAP’s structure which took a lot of work. The main script is `code/MAIN_transport_leap_import.py`. It relies heavily on dictionary based mappings defined in `code/transport_leap_mappings.py` and similar files.

## What’s in the folders
- `code/` Python scripts and code. Start with `code/MAIN_transport_leap_import.py` for transport; `code/MAIN_other_sectors.py` is intended as a starting point for handling non-transport tasks.
- `config/` environment file for setup (`env_leap.yml`) and the LEAP type library reference.
- `data/` input data such as esto balances and transport model files
- `intermediate_data/` checkpoints and other intermediate files created during processing.
- `results/` finished LEAP import/export files and reconciliation reports.
- `plotting_output/` charts (empty unless you run plotting steps).

## Quick start guide
1. Install Anaconda or Miniconda (Windows is required because the scripts talk to the LEAP desktop app).
2. Open an Anaconda Prompt and change into this folder, e.g. `cd C:\Users\Work\github\leap_utils_with_transport_toolkit`.
3. Create the Python environment: `conda env create --prefix ./env_leap --file ./config/env_leap.yml`.
4. Activate it each time you work: `conda activate ./env_leap`.

## what are the #%% blocks about?
- The code files are organized into cells (blocks) marked by `#%%` so you can run them interactively in IDEs like VS Code using jupyter interactive. You can run the whole script at once from the command line, but if you open it in an IDE you can run one block at a time to explore the code and see intermediate results. Ask chatgpt if you want to know more about this.
- The style used in the code helps to promote a faster, more interactive workflow for data exploration and analysis while still utilising concepts from software engineering such as modular functions and reusability. You will notice that a lot of functions are used, this is to help separate large amounts of code to help compartmentalise functionality. The functions are defined at the start of the scripts or even through imported scripts. They are then normally called in sequence via a MAIN function or script. This all helps to keep the code cleaner than a series of messy scripts and easier to read plus faster to write than code which relies more heavily on classes and object oriented programming.
- The functions can all be tested by using the debug option in jupyter interactive (which is why there are breakpoint() calls everywhere). You can use the 'debug cell' option next to each block in VS Code to step through the code line by line which is useful for inspecting the functions. 

## Tips
- Open LEAP if you plan to push values directly via COM.
- If the environment creation fails, ensure you ran the command from inside this folder so `./config/env_leap.yml` is found.
- Excel/CSV and other data-ey files are ignored by Git through the .gitignore file;
