
1. Add an SSH key named `wikidata-filter-key`

2. Ensure the server exists:

    ```shell
    ansible-playbook setup.yaml
    ````

3. Check the server exists in the inventory:

    ```shell
    ansible-inventory --graph -i vultr.yml
    ```

4. Start the job:

   ```
   ansible-playbook run.yaml --inventory vultr.yml -e "dump_date=20201230"
   ```

5. Upload results

   ```
   ssh ...
   curl -LOs https://archive.org/download/ia-pex/ia
   chmod +x ia
   ./ia configure
   rm wikidata-20210310-truthy-BETA.nt.bz2
   ./ia upload  wikidata-20201230-truthy-BETA-nt-filtered-en *.nt.bz2 --retries 10
   ./ia upload  wikidata-20201230-truthy-BETA-nt-labels-en labels_*.bz2 --retries 10
   ```
