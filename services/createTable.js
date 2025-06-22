const { db } = require('../routes/db');

const createTableForProduct = async (productName, fileName) => {
    try {
        ;
        let monthMatch = fileName.match(/\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b/);
        let month = monthMatch ? monthMatch[0] : new Date().toLocaleString("en-US", { month: "short" });

        let timestamp = `${month}`;

        let leadsTableName = `${productName.replace(/[-\s]/g, "_")}_leads_table_${timestamp}`;
        let salesTableName = `${productName.replace(/[-\s]/g, "_")}_sales_table_${timestamp}`;
        let loginTableName = `${productName.replace(/[-\s]/g, "_")}_login_table_${timestamp}`;
        let approvalTableName = `${productName.replace(/[-\s]/g, "_")}_approval_table_${timestamp}`;
        let disbTableName = `${productName.replace(/[-\s]/g, "_")}_disb_table_${timestamp}`;

        let createLeadsQuery = "";
        let createSalesQuery = "";
        let createLoginQuery = "";
        let createApprovalQuery = "";
        let createDisbQuery = "";

        if (productName === "Acko-car") {
            createLeadsQuery = `
                CREATE TABLE IF NOT EXISTS ${leadsTableName} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_date DATE,
                    phone VARCHAR(50) UNIQUE,
                    utm_campaign VARCHAR(20),
                    utm_medium VARCHAR(255),
                    source VARCHAR(30),
                    utm_term VARCHAR(20),
                    product VARCHAR(30),
                    quote_load VARCHAR(30),
                    city VARCHAR(50),
                    city_category VARCHAR(30),
                    gmb_definition VARCHAR(20)
                );
            `;

            createSalesQuery = `
                CREATE TABLE IF NOT EXISTS ${salesTableName} (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    report_date DATE,
                    phone VARCHAR(50) UNIQUE,
                    utm_campaign VARCHAR(20),
                    utm_medium VARCHAR(255),
                    source VARCHAR(30),
                    utm_term VARCHAR(20),
                    product VARCHAR(20),
                    gwp DECIMAL(10,2),
                    city VARCHAR(30),
                    city_category VARCHAR(20),
                    amount DECIMAL(10,2)
                );
            `;
        } else if (productName === "Club Mahindra") {
            createLeadsQuery = `

            CREATE TABLE IF NOT EXISTS ${leadsTableName} (
                lead_date varchar(20),
                data_source varchar(20),
                reffer_id_source varchar(100),
                camp_type varchar(25),
                owner_user_id varchar(20),
                lead_id varchar(20),
                lead_status varchar(30),
                lead_category varchar(10),
                camp_name varchar(50),
                call_status varchar(20),
                city varchar(15),
                tg varchar(10),
                state varchar(20),
                branch varchar(20),
                car_model varchar(10),
                car_make varchar(20),
                callback_date varchar(30),
                lead_position varchar(30),
                total_no_of_call_attempts varchar(5),
                fresh_lead varchar(10),
                first_date varchar(30),
                last_date varchar(30),
                verified_flag varchar(5),
                verified_by varchar(5),
                verified_date varchar(5),
                active_flag varchar(5),
                last_updated_date varchar(30),
                remarks varchar(30),
                created_by varchar(5),
                not_interested varchar(5),
                no_of_call_attempts varchar(5),
                pincode varchar(10),
                follow_up varchar(20),
                not_qualified varchar(20),
                rejection varchar(20),
                lead_type varchar(15),
                control_location varchar(30),
                age_group varchar(10),
                keyword varchar(15),
                cmp varchar(10),
                placement varchar(20),
                event_string varchar(10),
                gclid varchar(5),
                gaid varchar(5),
                web_url varchar(255),
                whatsapp_consent varchar(10),
                consent_date varchar(30),
                concat_string varchar(30),
                utm_term varchar(20),
                family_size varchar(5),
                lead_score varchar(5),
                lead_stage varchar(10),
                nvuid varchar(30),
                browser_name varchar(20),
                browser_platform varchar(15),
                browser_version varchar(15)
            );

            CREATE TABLE IF NOT EXISTS \`${productName.replace(/[-\s]/g, "_")}_uploaded_files\` (
                date varchar(20),
                filename VARCHAR(255) NOT NULL,
                filepath TEXT NOT NULL,
                uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

        `;

        } else if (productName === "Bajaj HL") {
            createLeadsQuery = `
                CREATE TABLE IF NOT EXISTS ${leadsTableName} (
                    offer_id varchar(10),
                    offer_name varchar(15),
                    offer_product varchar(5),
                    business_vertical varchar(5),
                    bv varchar(5),
                    utm_source varchar(5),
                    utm_medium varchar(10),
                    utm_campaign varchar(30),
                    bt_f varchar(6),
                    utm_content varchar(20),
                    sourcing_branch varchar(15),
                    warm varchar(5),
                    channel varchar(10),
                    city varchar(15),
                    campaign_date varchar(15),
                    month varchar(5),
                    demart_status varchar(5),
                    vouchers varchar(5),
                    utm_product varchar(10),
                    turnover varchar(5),
                    total_experience varchar(5),
                    tele_followup varchar(5),
                    telecaller_remarks varchar(255),
                    telecaller_action varchar(5),
                    tele_userid varchar(10),
                    tele_disposition_1 varchar(5),
                    tele_disposition_2 varchar(30),
                    tele_disposition_3 varchar(5),
                    telecaller_dispositiontype varchar(10),
                    sale_status varchar(5),
                    responsed_date varchar(15),
                    required_loan_amount varchar(10),
                    property_location varchar(5),
                    property_identified varchar(5),
                    propensity varchar(5),
                    pan_no varchar(10),
                    owner_id varchar(15),
                    owner_name varchar(20),
                    offer_amount varchar(10),
                    not_salary varchar(10),
                    ndeureofbusiness varchar(15),
                    nameof_degree varchar(5),
                    degree_type varchar(5),
                    loan_type varchar(5),
                    lead_id varchar(12),
                    idest_cibil_mobile varchar(5),
                    isbt varchar(5),
                    hostleadid varchar(30),
                    gross_receipt varchar(10),
                    field_followup_date varchar(15),
                    field_userid varchar(10),
                    field_remarks varchar(50),
                    field_disposition_3 varchar(10),
                    field_disposition_2 varchar(5),
                    field_disposition_1 varchar(5),
                    sales_dispositiontype varchar(5),
                    field_action varchar(5),
                    enquiry_city varchar(15),
                    enquiry_product varchar(15),
                    employment_type varchar(10),
                    email_id varchar(20),
                    dnc_flag varchar(5),
                    customer_name varchar(255),
                    cibil_notes varchar(100),
                    camp_type varchar(10),
                    camp_name varchar(10),
                    mobile_no varchar(12),
                    alt_mobile_no varchar(12),
                    agreement_no varchar(12),
                    curr_experience varchar(5),
                    enquiry_datetime varchar(15),
                    requiredloan_amount varchar(10),
                    current_bank_name varchar(12),
                    sanctioned_loan_amount varchar(10),
                    rate_of_interest varchar(10),
                    interested_in varchar(5),
                    field_last3years varchar(5),
                    property_type varchar(5),
                    down_payment varchar(5),
                    processing_branch varchar(15),
                    zip_code varchar(10),
                    hold_date varchar(10),
                    hold_reason varchar(15),
                    date_of_birth varchar(15),
                    login_date varchar(15),
                    tele_disposition_date varchar(15),
                    disp_camp_date varchar(15),
                    log_camp_tde varchar(5),
                    rechurn_bv varchar(5),
                    rechurn_date varchar(5),
                    ok_tag varchar(5),
                    exclusion_reason varchar(40),
                    allocated_ownerid varchar(15),
                    fcc varchar(5),
                    present_ownerid varchar(10),
                    priority varchar(5),
                    bhfl_bfl varchar(5),
                    field varchar(5),
                    flag varchar(5),
                    rechurn_flag varchar(5),
                    p2_flag_field varchar(5),
                    rechurn_field varchar(5),
                    ticket_size varchar(5)
                );

                CREATE TABLE IF NOT EXISTS \`${productName.replace(/[-\s]/g, "_")}_uploaded_files\` (
                date varchar(20),
                filename VARCHAR(255) NOT NULL,
                filepath TEXT NOT NULL,
                uploaded_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            `;

            createLoginQuery = `
            CREATE TABLE IF NOT EXISTS ${loginTableName} (
               login_agreementno varchar(15),
               bussiness_vertical varchar(15),
               bv varchar(5),
               customer_id varchar(10),
               login_date varchar(15),
               month varchar(5),
               initiate_login_date varchar(15),
               approval_date varchar(15),
               colletral_date varchar(15),
               sourchannel_category varchar(5),
               loan_login_status varchar(20),
               revised_stage varchar(20),
               wip varchar(5),
               lead_pan varchar(5),
               lead_mobile_no varchar(12),
               lead_altmobile_no varchar(12),
               phn_no varchar(12),
               datamart_altmobile_no varchar(12),
               co_applicant1_phn varchar(12),
               co_applicant2_phn varchar(12),
               co_applicant3_phn varchar(12),
               pan_c varchar(5),
               co_applicant1_pancard varchar(5),
               co_applicant2_pancard varchar(5),
               co_applicant3_pancard varchar(5),
               login_status varchar(5),
               approved_loan_amount varchar(12),
               total_loan_amount varchar(12),
               loan_category varchar(5),
               product_code varchar(5),
               lead_source varchar(5),
               cust_cif varchar(10),
               disbursement_date varchar(15),
               branch_id varchar(5),
               city_name varchar(12),
               lead_id varchar(12),
               offer_id varchar(12),
               camp_type varchar(10),
               camp_name varchar(5),
               camp_date varchar(15),
               utm_source varchar(5),
               utm_medium varchar(10),
               utm_campaign varchar(30),
               utm_content varchar(15),
               utm_product varchar(5),
               offer_date varchar(15),
               offer_name varchar(15),
               offer_product varchar(5),
               product_offering_name varchar(5),
               owner_id varchar(10),
               datamart_status varchar(5),
               offer_amount varchar(5),
               po_business_vertical varchar(10),
               bt varchar(5),
               camp_tat varchar(5),
               vouchers varchar(5),
               response_type varchar(5),
               channel varchar(10),
               dispositionvl1_sales varchar(5),
               dispositionvl2_sales varchar(5),
               dispositionvl3_sales varchar(10),
               sales_status varchar(5),
               dispositionvl1_telecaller varchar(10),
               dispositionvl2_telecaller varchar(10),
               dispositionvl3_telecaller varchar(5),
               telecaller_dispositiontype varchar(10),
               tat varchar(5),
               flag varchar(5),
               initiate_login_tat varchar(5),
               initiate_login_flag varchar(5),
               lead_reference varchar(10),
               match_flag varchar(100),
               business_vertical_flag varchar(5),
               initiate_login_calc_tat varchar(5),
               calc_tat varchar(5),
               score varchar(5),
               cnt varchar(5),
               fcc varchar(5)
            );
        `;

            createApprovalQuery = createLoginQuery.replace(loginTableName, approvalTableName);
            createDisbQuery = createLoginQuery.replace(loginTableName, disbTableName);

        } else {
            throw new Error("Unsupported product type");
        }


        await db.query(createLeadsQuery);
        console.log(`Table '${leadsTableName}' created successfully!`);


        if (createSalesQuery) {
            await db.query(createSalesQuery);
            console.log(`Table '${salesTableName}' created successfully!`);
        }

        if (createLoginQuery) {
            await db.query(createLoginQuery);
            console.log(`Table '${loginTableName}' created successfully!`);
        }

        if (createApprovalQuery) {
            await db.query(createApprovalQuery);
            console.log(`Table '${approvalTableName}' created successfully!`);
        }

        if (createDisbQuery) {
            await db.query(createDisbQuery);
            console.log(`Table '${disbTableName}' created successfully!`);
        }


        const result = {
            leadsTableName: createLeadsQuery ? leadsTableName : null,
            salesTableName: createSalesQuery ? salesTableName : null,
            loginTableName: createLoginQuery ? loginTableName : null,
            approvalTableName: createApprovalQuery ? approvalTableName : null,
            disbTableName: createDisbQuery ? disbTableName : null
        };

        return result;

    } catch (err) {
        console.error("Error creating tables:", err);
        throw err;
    }
}

module.exports = createTableForProduct;