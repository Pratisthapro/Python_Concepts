class Runner(object):
    @staticmethod
    def runner(file_object):
        import pandas as pd
        import snowflake.connector
        import requests, json, os
        from tabulate import tabulate

        # --- Fetch credentials from DCM ---
        dcm_host = os.environ.get("IND_WORKFLOW_HOST")
        dcm_port = str(os.environ.get("IND_WORKFLOW_PORT"))
        datastore_name = "warehouse"
        response = requests.get(
            f"http://{dcm_host}:{dcm_port}/collection/credential?datastore_name={datastore_name}&user_id=dap_user"
        )
        credentials = json.loads(response.content)["data"]

        host = credentials['host']
        user = credentials['username']
        password = credentials['password']

        # --- Connect to Snowflake ---
        con = snowflake.connector.connect(
            user=user,
            password=password,
            account='cwhdsmpolloprod',
            warehouse='OLALOODWAREHOUSE',
            database='DAP',
            schema='L2'
        )
        cur = con.cursor()

        # --- Query ---
        query = '''((WITH current_data AS (
            SELECT 'Current Time'  AS "CATEGORY NAME",
                   CAST(CURRENT_DATE AS VARCHAR)  AS "DATE",
                   NULL::INTEGER AS "NO. OF RECORDS"
            UNION ALL
            SELECT 'Appointment', 
					CAST(MAX(efdt) AS VARCHAR), 
					COUNT(*)::BIGINT
            FROM L2.PD_ACTIVITY
            WHERE SSTP = 'INTERLACHEN PEDIATRICS' AND CLTP = 'appointment' AND ingdt IN (SELECT max(ingdt) FROM L2.PD_ACTIVITY WHERE SSTP = 'INTERLACHEN PEDIATRICS' AND CLTP = 'appointment')
)
        SELECT 
            c."CATEGORY NAME",
            c."DATE",
            c."NO. OF RECORDS" AS "CURR REC",
            b."NO. OF RECORDS" AS "PREV REC",
            CASE 
                WHEN b."NO. OF RECORDS" IS NULL OR b."NO. OF RECORDS" = 0 THEN NULL
                ELSE ROUND(100 * (c."NO. OF RECORDS" - b."NO. OF RECORDS") / b."NO. OF RECORDS", 2)
            END AS "% DEVIATION"
        FROM current_data c
        LEFT JOIN alert_backup_eCW_Interlachen_Pediatrics_appointment b 
            ON c."CATEGORY NAME" = b."CATEGORY NAME"
        ORDER BY c."NO. OF RECORDS" desc))'''

        df = pd.read_sql_query(query, con)
		
		# ---- Fix scientific notation for Slack ---

        for col in ['CURR REC', 'PREV REC']:
            df[col] = df[col].apply(
            lambda x: f"{int(x):,}" if pd.notnull(x) else ""
            )

        df['% DEVIATION'] = df['% DEVIATION'].apply(
        lambda x: f"{x:,.2f}" if pd.notnull(x) else ""
        )

        # --- Close connections ---
        cur.close()
        con.close()

        # --- Convert DataFrame to Markdown-style table ---
        table_str = tabulate(df, headers="keys", tablefmt="github", showindex=False)

        # --- Slack Block message ---
        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": " DAP Notification Alert"}
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "*Appointment - eCW INTERLACHEN PEDIATRICS Integration Alert*"}
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"```{table_str}```"}
                }
            ]
        }

        # --- Post to Slack ---
        url = "https://hooks.slack.com/services/Y/CQ1LtC3mBDD3"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(payload))

        yield 'abc'
