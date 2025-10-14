this code is for shifting data from the 9th edition datasets to the LEAP framework (https://leap.sei.org/)

The task is meant for a one-time data transformation, so the code is not optimized for speed or efficiency. However the expectation is that this project will require full doucmentation of the data structures and plans for the LEAP modelling project so that in the futre, other data transfomrations on top of LEAP, or using the outputs from LEAP can be done with ease. In addition to doucmetnation. 

As a start the code we will be focusing on will be for transferring the transportation data from the 9th edition to LEAP. This contains a lot of detail and is quite complex.

Generally we will use the following major defintions:
Source data: the 9th edition datasets
LEAP data: the data as it is represented in LEAP

Instructions for CODEX AGENTS:
1. Write modular code: Break down the code into smaller, reusable functions or classes that perform specific tasks. This will make it easier to understand, test, and maintain.
2. Use clear and descriptive names: Choose meaningful names for variables, functions, and classes that convey their purpose. This will enhance code readability and make it easier for others (and yourself) to understand the code later.
3. Add comments and documentation: Include comments throughout the code to explain the logic, purpose, and any complex sections. Additionally, provide documentation for functions and classes to describe their inputs, outputs, and behavior.
4. Finns Jupyter interactive notebooks Coding style: Finn uses the jupyter interactive program on top of .py files. So code should be wrapped in #%%'s to allow for easy running and testing of code blocks. The code should be structured in a way so that files are used to split major sections of code so general legnth of each file is less than 300 lines (max 500 lines). The workflow should be to have a central .py file, e.g. LEAP_transfers_transport.py which will utilise its own functions and classes, as well as importing functions and classes from other files, e.g. LEAP_transport_measures_config.py, LEAP_transfers_transport_MAPPINGS.py etc. and the workflow should be at the bottom of the file so its easy to find and run.
5. Finn likes to use a lot of dictionaries to store mappings and configs so as to make it clear how the data is structured and how it is being transformed. So please use dictionaries where appropriate. this is also important for the documentation of the data structures for ai agents in the future.
