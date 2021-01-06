
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
   ansible-playbook playbook.yaml --inventory vultr.yml -e "dump_date=20201230"
   ```
