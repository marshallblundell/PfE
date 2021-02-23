# PfE
How Should California Pay for Electricity? Efficiency and Distributional Considerationsof Alternative Funding Mechanisms

Code contained in the replication package can be used to build the data files and generate all figures in the paper.

Some steps of the analysis are encoded as functions that the code does not run by default. Rather, the repository includes output from these steps in the "ModifiedData" folder. This speeds up run time and means steps that make calls to API or do intensive data cleaning are only performed once. These steps include:

1) BTM PV production simulation using LBNL tracking the sun data and PV Watts
2) Ferc form 1 data cleaning
3) Gas prices cleaning
4) Constructing 2019 rates using advice letters/tariff sheets data.

If you do want to run the BTM PV production simulation, you must download LBNL's 2019 tracking the sun data and put it in the OriginalData\lbnl_pv_data_public_tts folder: https://emp.lbl.gov/tracking-the-sun
I would, however, recommend against re-running the BTM PV simulation, as the run-time is long due to rate limits on the PV Watts API. You would also need to generate your own key with PV Watts.



