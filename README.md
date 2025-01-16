# Soundless Batch task

This code is hosted on the virtual machine that runs once a day, it converts the data in Google Cloud Pub/Sub into csv files that will be stored in the corresponding storage.

Please consider:
- This repository is expected to be linked to Google Cloud Build. Commits to the main branch will containerize and upload images to Artifact Registry with the `:latest` tag. (See steps in `cloudbuild.yaml`)
- The `batch_pubsub_to_cos.py` task is run sequentially and in batches. It can be prolongued indefinitely as long as there are measurements to process.
- `town_boundaries` files are needed to group measurements in files depending on its location (only Catalan towns). They are extracted from the Institut Cartogràfic i Geològic de Catalunya (ICGC) website.
