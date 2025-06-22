
const getLoadQueryForProduct = (productName, leadsCSVPath, salesCSVPath, loginCSVPath, approvalCSVPath, disbCSVPath, leadsTableName, salesTableName, loginTableName, approvalTableName, disbTableName) => {
    let loadQuery = "";

    if (productName === "Acko-car") {

        loadQuery = `
        LOAD DATA LOCAL INFILE '${leadsCSVPath}'
        INTO TABLE ${leadsTableName} 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 ROWS 
        (report_date, phone, utm_campaign, utm_medium, source, utm_term, product, quote_load, city, city_category, gmb_definition)
        set report_date = str_to_date(report_date, '%Y-%m-%d');

        INSERT IGNORE INTO ${leadsTableName} (report_date, phone, utm_campaign, utm_medium, source, utm_term, product, quote_load, city, city_category, gmb_definition)
        SELECT report_date, phone, utm_campaign, utm_medium, source, utm_term, product, quote_load, city, city_category, gmb_definition FROM ${leadsTableName};


        LOAD DATA LOCAL INFILE '${salesCSVPath}' 
        INTO TABLE ${salesTableName} 
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"' 
        LINES TERMINATED BY '\n' 
        IGNORE 1 ROWS 
        (report_date, phone, utm_campaign, utm_medium, source, utm_term, product, gwp, city, city_category, amount)
        set report_date = str_to_date(report_date, '%Y-%m-%d');

        INSERT IGNORE INTO ${salesTableName} (report_date, phone, utm_campaign, utm_medium, source, utm_term, product, gwp, city, city_category, amount)
        SELECT report_date, phone, utm_campaign, utm_medium, source, utm_term, product, gwp, city, city_category, amount FROM ${salesTableName};
        `;
    } else if (productName === "Club Mahindra") {
        loadQuery = `
            TRUNCATE TABLE ${leadsTableName};

            DROP TEMPORARY TABLE IF EXISTS temp_leads;
            CREATE TEMPORARY TABLE temp_leads LIKE ${leadsTableName};


            LOAD DATA LOCAL INFILE '${leadsCSVPath}'
            INTO TABLE temp_leads
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\n' 
            IGNORE 1 ROWS 
            (lead_date, data_source, reffer_id_source, camp_type, owner_user_id, lead_id, lead_status, lead_category, camp_name, call_status, city, tg, state, branch, car_model, car_make, callback_date, lead_position, total_no_of_call_attempts, fresh_lead, first_date, last_date, verified_flag, verified_by, verified_date, active_flag, last_updated_date, 
            remarks, created_by, not_interested, no_of_call_attempts, pincode, follow_up, not_qualified, rejection, lead_type, control_location, age_group, keyword, cmp, placement, event_string, gclid, gaid, web_url, whatsapp_consent, consent_date, concat_string, utm_term, family_size, lead_score, lead_stage, nvuid, browser_name, browser_platform, browser_version)
            SET lead_date = STR_TO_DATE(lead_date, '%d-%b-%y'), 
                first_date = STR_TO_DATE(first_date, '%Y-%m-%d %H:%i:%s'), 
                last_date = STR_TO_DATE(last_date, '%Y-%m-%d %H:%i:%s'), 
                last_updated_date = STR_TO_DATE(last_updated_date, '%Y-%m-%d %H:%i:%s'), 
                consent_date = STR_TO_DATE(consent_date, '%Y-%m-%d %H:%i:%s');


            INSERT INTO ${leadsTableName} 
            SELECT t.* FROM temp_leads t
            LEFT JOIN ${leadsTableName} l ON t.lead_id = l.lead_id
            WHERE l.lead_id IS NULL;


            DROP TEMPORARY TABLE IF EXISTS temp_leads;

        `;
    } else if (productName === "Bajaj HL") {
        loadQuery = `
            DROP TEMPORARY TABLE IF EXISTS temp_leads;
            CREATE TEMPORARY TABLE temp_leads LIKE ${leadsTableName};


            LOAD DATA LOCAL INFILE '${leadsCSVPath}'
            INTO TABLE temp_leads
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\n' 
            IGNORE 1 ROWS 
            (offer_id, offer_name, offer_product, business_vertical, bv, utm_source, utm_medium, utm_campaign, bt_f, utm_content, sourcing_branch, warm, channel, city, campaign_date, month, demart_status, vouchers, utm_product, turnover, total_experience, tele_followup, telecaller_remarks, telecaller_action, tele_userid, tele_disposition_1, tele_disposition_2, tele_disposition_3, 
            telecaller_dispositiontype, sale_status, responsed_date, required_loan_amount, property_location, property_identified, propensity, pan_no, owner_id, owner_name, offer_amount, not_salary, ndeureofbusiness, nameof_degree, degree_type, loan_type, lead_id, idest_cibil_mobile, isbt, hostleadid, gross_receipt, field_followup_date, field_userid, field_remarks, field_disposition_3, 
            field_disposition_2, field_disposition_1, sales_dispositiontype, field_action, enquiry_city, enquiry_product, employment_type, email_id, dnc_flag, customer_name, cibil_notes, camp_type, camp_name, mobile_no, alt_mobile_no, agreement_no, curr_experience, enquiry_datetime, requiredloan_amount, current_bank_name, sanctioned_loan_amount, rate_of_interest, interested_in, field_last3years,
            property_type, down_payment, processing_branch, zip_code, hold_date, hold_reason, date_of_birth, login_date, tele_disposition_date, disp_camp_date, log_camp_tde, rechurn_bv, rechurn_date, ok_tag, exclusion_reason, allocated_ownerid, fcc, present_ownerid, priority, bhfl_bfl, field, flag, rechurn_flag, p2_flag_field, rechurn_field, ticket_size)
            SET campaign_date = STR_TO_DATE(campaign_date, '%m/%d/%Y'), 
                responsed_date = STR_TO_DATE(responsed_date, '%m/%d/%Y'), 
                field_followup_date = STR_TO_DATE(field_followup_date, '%m/%d/%Y'), 
                date_of_birth = STR_TO_DATE(date_of_birth, '%m/%d/%Y'), 
                login_date = STR_TO_DATE(login_date, '%m/%d/%Y'),
                tele_disposition_date = STR_TO_DATE(tele_disposition_date, '%m/%d/%Y');


            INSERT INTO ${leadsTableName} 
            SELECT t.* FROM temp_leads t
            LEFT JOIN ${leadsTableName} l ON t.offer_id = l.offer_id
            WHERE l.offer_id IS NULL;

            DROP TEMPORARY TABLE IF EXISTS temp_leads;

            DROP TEMPORARY TABLE IF EXISTS temp_login;
            CREATE TEMPORARY TABLE temp_login LIKE ${loginTableName};


            LOAD DATA LOCAL INFILE '${loginCSVPath}'
            INTO TABLE temp_login
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\n' 
            IGNORE 1 ROWS 
            (login_agreementno, bussiness_vertical, bv, customer_id, login_date, month, initiate_login_date, approval_date, colletral_date, sourchannel_category, loan_login_status, revised_stage, wip, lead_pan, lead_mobile_no, lead_altmobile_no, phn_no, datamart_altmobile_no, co_applicant1_phn, co_applicant2_phn, co_applicant3_phn,
            pan_c, co_applicant1_pancard, co_applicant2_pancard, co_applicant3_pancard, login_status, approved_loan_amount, total_loan_amount, loan_category, product_code, lead_source, cust_cif, disbursement_date, branch_id, city_name, lead_id, offer_id, camp_type, camp_name, camp_date, utm_source, utm_medium, utm_campaign, utm_content, 
            utm_product, offer_date, offer_name, offer_product, product_offering_name, owner_id, datamart_status, offer_amount, po_business_vertical, bt, camp_tat, vouchers, response_type, channel, dispositionvl1_sales, dispositionvl2_sales, dispositionvl3_sales, sales_status, dispositionvl1_telecaller, dispositionvl2_telecaller, dispositionvl3_telecaller, telecaller_dispositiontype, 
            tat, flag, initiate_login_tat, initiate_login_flag, lead_reference, match_flag, business_vertical_flag, initiate_login_calc_tat, calc_tat, score, cnt, fcc)
            SET login_date = STR_TO_DATE(login_date, '%m/%d/%Y'), 
                initiate_login_date = STR_TO_DATE(initiate_login_date, '%m/%d/%Y'), 
                camp_date = STR_TO_DATE(camp_date, '%m/%d/%Y'), 
                offer_date = STR_TO_DATE(offer_date, '%m/%d/%Y'); 
                


            INSERT INTO ${loginTableName} 
            SELECT t.* FROM temp_login t
            LEFT JOIN ${loginTableName} l ON t.customer_id = l.customer_id
            WHERE l.customer_id IS NULL;

            DROP TEMPORARY TABLE IF EXISTS temp_login;

            DROP TEMPORARY TABLE IF EXISTS temp_approval;
            CREATE TEMPORARY TABLE temp_approval LIKE ${approvalTableName};


            LOAD DATA LOCAL INFILE '${approvalCSVPath}'
            INTO TABLE temp_approval
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\n' 
            IGNORE 1 ROWS 
            (login_agreementno, bussiness_vertical, bv, customer_id, login_date, month, initiate_login_date, approval_date, colletral_date, sourchannel_category, loan_login_status, revised_stage, wip, lead_pan, lead_mobile_no, lead_altmobile_no, phn_no, datamart_altmobile_no, co_applicant1_phn, co_applicant2_phn, co_applicant3_phn,
            pan_c, co_applicant1_pancard, co_applicant2_pancard, co_applicant3_pancard, login_status, approved_loan_amount, total_loan_amount, loan_category, product_code, lead_source, cust_cif, disbursement_date, branch_id, city_name, lead_id, offer_id, camp_type, camp_name, camp_date, utm_source, utm_medium, utm_campaign, utm_content, 
            utm_product, offer_date, offer_name, offer_product, product_offering_name, owner_id, datamart_status, offer_amount, po_business_vertical, bt, camp_tat, vouchers, response_type, channel, dispositionvl1_sales, dispositionvl2_sales, dispositionvl3_sales, sales_status, dispositionvl1_telecaller, dispositionvl2_telecaller, dispositionvl3_telecaller, telecaller_dispositiontype, 
            tat, flag, initiate_login_tat, initiate_login_flag, lead_reference, match_flag, business_vertical_flag, initiate_login_calc_tat, calc_tat, score, cnt, fcc)
            SET login_date = STR_TO_DATE(login_date, '%m/%d/%Y'), 
                initiate_login_date = STR_TO_DATE(initiate_login_date, '%m/%d/%Y'), 
                camp_date = STR_TO_DATE(camp_date, '%m/%d/%Y'), 
                offer_date = STR_TO_DATE(offer_date, '%m/%d/%Y'); 
                


            INSERT INTO ${approvalTableName} 
            SELECT t.* FROM temp_approval t
            LEFT JOIN ${approvalTableName} l ON t.customer_id = l.customer_id
            WHERE l.customer_id IS NULL;

            DROP TEMPORARY TABLE IF EXISTS temp_approval;

            DROP TEMPORARY TABLE IF EXISTS temp_disb;
            CREATE TEMPORARY TABLE temp_disb LIKE ${disbTableName};


            LOAD DATA LOCAL INFILE '${disbCSVPath}'
            INTO TABLE temp_disb
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"' 
            LINES TERMINATED BY '\n' 
            IGNORE 1 ROWS 
            (login_agreementno, bussiness_vertical, bv, customer_id, login_date, month, initiate_login_date, approval_date, colletral_date, sourchannel_category, loan_login_status, revised_stage, wip, lead_pan, lead_mobile_no, lead_altmobile_no, phn_no, datamart_altmobile_no, co_applicant1_phn, co_applicant2_phn, co_applicant3_phn,
            pan_c, co_applicant1_pancard, co_applicant2_pancard, co_applicant3_pancard, login_status, approved_loan_amount, total_loan_amount, loan_category, product_code, lead_source, cust_cif, disbursement_date, branch_id, city_name, lead_id, offer_id, camp_type, camp_name, camp_date, utm_source, utm_medium, utm_campaign, utm_content, 
            utm_product, offer_date, offer_name, offer_product, product_offering_name, owner_id, datamart_status, offer_amount, po_business_vertical, bt, camp_tat, vouchers, response_type, channel, dispositionvl1_sales, dispositionvl2_sales, dispositionvl3_sales, sales_status, dispositionvl1_telecaller, dispositionvl2_telecaller, dispositionvl3_telecaller, telecaller_dispositiontype, 
            tat, flag, initiate_login_tat, initiate_login_flag, lead_reference, match_flag, business_vertical_flag, initiate_login_calc_tat, calc_tat, score, cnt, fcc)
            SET login_date = STR_TO_DATE(login_date, '%m/%d/%Y'), 
                initiate_login_date = STR_TO_DATE(initiate_login_date, '%m/%d/%Y'), 
                camp_date = STR_TO_DATE(camp_date, '%m/%d/%Y'), 
                offer_date = STR_TO_DATE(offer_date, '%m/%d/%Y'); 
                


            INSERT INTO ${disbTableName} 
            SELECT t.* FROM temp_disb t
            LEFT JOIN ${disbTableName} l ON t.customer_id = l.customer_id
            WHERE l.customer_id IS NULL;

            DROP TEMPORARY TABLE IF EXISTS temp_disb;

        `;
    } else {
        throw new Error("Unsupported product type");
    }

    return loadQuery;
};

module.exports = getLoadQueryForProduct;