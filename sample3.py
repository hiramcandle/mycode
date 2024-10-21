from functools import partial
from direct_summarizer.delivery.Delivery import Delivery
from snowpark_common_library.utils.utils import deep_get as get_dict_element
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import IntegerType


all_col_names = ["delivery_wid",
                "line_item_lineage_id",
                "delivery_data_representation_id",
                "meta_ad_set_wid",
                "meta_campaign_wid",
                "delivered_on_date",
                "integration_source_code",
                "agency_id",
                "meta_ad_set_id",
                "meta_campaign_id",
                "meta_account_id",
                "attribution_setting",
                "campaign_type",
                "src_fb_offline_purchases",
                "src_post_engagement",
                "src_views",
                "src_link_clicks",
                "src_fb_mobile_purchase",
                "src_total_actions",
                "src_fb_pixel_add_to_cart",
                "src_new_messaging_connections",
                "src_purchase",
                "src_purchase_value",
                "src_unique_clicks",
                "src_frequency",
                "src_comments",
                "src_imps",
                "src_clicks",
                "src_link_clicks_actions",
                "src_video_avg_time_watched_actions",
                "src_reach",
                "src_video_30_sec_watched_actions",
                "src_shares",
                "src_page_likes",
                "src_video_view_3s",
                "src_add_to_cart",
                "src_initiate_checkout",
                "src_video_p95_watched_actions",
                "src_inline_post_engagement",
                "src_post_reactions",
                "src_spend",
                "src_video_play_actions_view_value",
                "src_all_clicks",
                "src_average_play_time_count",
                "src_unique_video_view",
                "src_fb_pixel_purchase",
                "src_daily_budget",
                "src_lifetime_budget",
                "src_lifetime_budget_with_offset",
                "src_daily_budget_with_offset",
                "src_add_payment_info",
                "src_add_to_wishlist",
                "src_app_use",
                "src_complete_registration",
                "src_fb_mobile_add_payment_info",
                "src_fb_mobile_complete_registration",
                "src_fb_mobile_content_view",
                "src_fb_offline_purchase_conv_value",
                "src_landing_page_view",
                "src_lead",
                "src_leads",
                "src_mobile_app_install",
                "src_search",
                "src_thru_play",
                "src_video_p100_watched_actions",
                "src_video_p25_watched_actions",
                "src_video_p50_watched_actions",
                "src_video_p75_watched_actions",
                "src_view_content",
                "src_lead_grouped",
                "src_event_responses",
                "src_page_engagement",
                "src_unique_landing_page_view",
                "src_contact_total",
                "src_donate_total",
                "src_schedule_total",
                "src_start_trial_total",
                "src_submit_application_total",
                "src_subscribe_total",
                "src_unique_chekin",
                "src_unique_link_click",
                "src_unique_page_engagement",
                "src_unique_like",
                "src_unique_comment",
                "src_unique_post_engagement",
                "src_unique_post_reactions",
                "src_unique_shares",
                "date_wid",
                "data_as_of_ts",
                "file_name",
                "file_row_number",
                "created_batch_id",
                "created_ts",
                "modified_batch_id",
                "modified_ts"]


class DeliveryMetaAdSet(Delivery):
    '''loads fct_delivery_meta_lineitem_adset_daily_mapped'''

    def merge_data(self, target, mapped_delivery_df):
        '''final merge statement'''
        target.merge(mapped_delivery_df,
                    ((target["line_item_lineage_id"] == mapped_delivery_df["line_item_lineage_id"]) &
                    (target["agency_id"] == mapped_delivery_df["agency_id"]) &
                    (target["delivery_data_representation_id"] == mapped_delivery_df["delivery_data_representation_id"]) &
                    (target["meta_ad_set_id"] == mapped_delivery_df["meta_ad_set_id"]) &
                    (target["meta_campaign_id"] == mapped_delivery_df["meta_campaign_id"]) &
                    (target["delivered_on_date"] == mapped_delivery_df["delivered_on_date"])),
                    [F.when_matched().update({
                                            "attribution_setting": mapped_delivery_df["attribution_setting"],
                                            "campaign_type" : mapped_delivery_df["campaign_type"],
                                            "src_fb_offline_purchases" : mapped_delivery_df["src_fb_offline_purchases"],
                                            "src_post_engagement" : mapped_delivery_df["src_post_engagement"],
                                            "src_views" : mapped_delivery_df["src_views"],
                                            "src_link_clicks" : mapped_delivery_df["src_link_clicks"],
                                            "src_fb_mobile_purchase" : mapped_delivery_df["src_fb_mobile_purchase"],
                                            "src_total_actions" : mapped_delivery_df["src_total_actions"],
                                            "src_fb_pixel_add_to_cart" : mapped_delivery_df["src_fb_pixel_add_to_cart"],
                                            "src_new_messaging_connections" : mapped_delivery_df["src_new_messaging_connections"],
                                            "src_purchase" : mapped_delivery_df["src_purchase"],
                                            "src_purchase_value" : mapped_delivery_df["src_purchase_value"],
                                            "src_unique_clicks" : mapped_delivery_df["src_unique_clicks"],
                                            "src_frequency" : mapped_delivery_df["src_frequency"],
                                            "src_comments" : mapped_delivery_df["src_comments"],
                                            "src_imps" : mapped_delivery_df["src_imps"],
                                            "src_clicks" : mapped_delivery_df["src_clicks"],
                                            "src_link_clicks_actions" : mapped_delivery_df["src_link_clicks_actions"],
                                            "src_video_avg_time_watched_actions" : mapped_delivery_df["src_video_avg_time_watched_actions"],
                                            "src_reach" : mapped_delivery_df["src_reach"],
                                            "src_video_30_sec_watched_actions" : mapped_delivery_df["src_video_30_sec_watched_actions"],
                                            "src_shares" : mapped_delivery_df["src_shares"],
                                            "src_page_likes" : mapped_delivery_df["src_page_likes"],
                                            "src_video_view_3s" : mapped_delivery_df["src_video_view_3s"],
                                            "src_add_to_cart" : mapped_delivery_df["src_add_to_cart"],
                                            "src_initiate_checkout" : mapped_delivery_df["src_initiate_checkout"],
                                            "src_video_p95_watched_actions" : mapped_delivery_df["src_video_p95_watched_actions"],
                                            "src_inline_post_engagement" : mapped_delivery_df["src_inline_post_engagement"],
                                            "src_post_reactions" : mapped_delivery_df["src_post_reactions"],
                                            "src_spend" : mapped_delivery_df["src_spend"],
                                            "src_video_play_actions_view_value" : mapped_delivery_df["src_video_play_actions_view_value"],
                                            "src_all_clicks" : mapped_delivery_df["src_all_clicks"],
                                            "src_average_play_time_count" : mapped_delivery_df["src_average_play_time_count"],
                                            "src_unique_video_view" : mapped_delivery_df["src_unique_video_view"],
                                            "src_fb_pixel_purchase" : mapped_delivery_df["src_fb_pixel_purchase"],
                                            "src_daily_budget" : mapped_delivery_df["src_daily_budget"],
                                            "src_lifetime_budget" : mapped_delivery_df["src_lifetime_budget"],
                                            "src_lifetime_budget_with_offset" : mapped_delivery_df["src_lifetime_budget_with_offset"],
                                            "src_daily_budget_with_offset" : mapped_delivery_df["src_daily_budget_with_offset"],
                                            "src_add_payment_info" : mapped_delivery_df["src_add_payment_info"],
                                            "src_add_to_wishlist" : mapped_delivery_df["src_add_to_wishlist"],
                                            "src_app_use" : mapped_delivery_df["src_app_use"],
                                            "src_complete_registration" : mapped_delivery_df["src_complete_registration"],
                                            "src_fb_mobile_add_payment_info" : mapped_delivery_df["src_fb_mobile_add_payment_info"],
                                            "src_fb_mobile_complete_registration" : mapped_delivery_df["src_fb_mobile_complete_registration"],
                                            "src_fb_mobile_content_view" : mapped_delivery_df["src_fb_mobile_content_view"],
                                            "src_fb_offline_purchase_conv_value" : mapped_delivery_df["src_fb_offline_purchase_conv_value"],
                                            "src_landing_page_view" : mapped_delivery_df["src_landing_page_view"],
                                            "src_lead" : mapped_delivery_df["src_lead"],
                                            "src_leads" : mapped_delivery_df["src_leads"],
                                            "src_mobile_app_install" : mapped_delivery_df["src_mobile_app_install"],
                                            "src_search" : mapped_delivery_df["src_search"],
                                            "src_thru_play" : mapped_delivery_df["src_thru_play"],
                                            "src_video_p100_watched_actions" : mapped_delivery_df["src_video_p100_watched_actions"],
                                            "src_video_p25_watched_actions" : mapped_delivery_df["src_video_p25_watched_actions"],
                                            "src_video_p50_watched_actions" : mapped_delivery_df["src_video_p50_watched_actions"],
                                            "src_video_p75_watched_actions" : mapped_delivery_df["src_video_p75_watched_actions"],
                                            "src_view_content" : mapped_delivery_df["src_view_content"],
                                            "src_lead_grouped" : mapped_delivery_df["src_lead_grouped"],
                                            "src_event_responses" : mapped_delivery_df["src_event_responses"],
                                            "src_page_engagement" : mapped_delivery_df["src_page_engagement"],
                                            "src_unique_landing_page_view" : mapped_delivery_df["src_unique_landing_page_view"],
                                            "src_contact_total" : mapped_delivery_df["src_contact_total"],
                                            "src_donate_total" : mapped_delivery_df["src_donate_total"],
                                            "src_schedule_total" : mapped_delivery_df["src_schedule_total"],
                                            "src_start_trial_total" : mapped_delivery_df["src_start_trial_total"],
                                            "src_submit_application_total" : mapped_delivery_df["src_submit_application_total"],
                                            "src_subscribe_total" : mapped_delivery_df["src_subscribe_total"],
                                            "src_unique_chekin" : mapped_delivery_df["src_unique_chekin"],
                                            "src_unique_link_click" : mapped_delivery_df["src_unique_link_click"],
                                            "src_unique_page_engagement" : mapped_delivery_df["src_unique_page_engagement"],
                                            "src_unique_like" : mapped_delivery_df["src_unique_like"],
                                            "src_unique_comment" : mapped_delivery_df["src_unique_comment"],
                                            "src_unique_post_engagement" : mapped_delivery_df["src_unique_post_engagement"],
                                            "src_unique_post_reactions" : mapped_delivery_df["src_unique_post_reactions"],
                                            "src_unique_shares" : mapped_delivery_df["src_unique_shares"],
                                            "data_as_of_ts" : mapped_delivery_df["data_as_of_ts"],
                                            "file_name": mapped_delivery_df["file_name"],
                                            "file_row_number": mapped_delivery_df["file_row_number"],
                                            "modified_batch_id" : mapped_delivery_df["modified_batch_id"],
                                            "modified_ts" : mapped_delivery_df["modified_ts"]
                                            }),
                        F.when_not_matched().insert({"delivery_wid" : mapped_delivery_df["delivery_wid"],
                                            "line_item_lineage_id" : mapped_delivery_df["line_item_lineage_id"],
                                            "delivery_data_representation_id" : mapped_delivery_df["delivery_data_representation_id"],
                                            "meta_ad_set_wid" : mapped_delivery_df["meta_ad_set_wid"],
                                            "meta_campaign_wid" : mapped_delivery_df["meta_campaign_wid"],
                                            "delivered_on_date" : mapped_delivery_df["delivered_on_date"],
                                            "integration_source_code" : mapped_delivery_df["integration_source_code"],
                                            "agency_id" : mapped_delivery_df["agency_id"],
                                            "meta_ad_set_id" : mapped_delivery_df["meta_ad_set_id"],
                                            "meta_campaign_id" : mapped_delivery_df["meta_campaign_id"],
                                            "meta_account_id": mapped_delivery_df["meta_account_id"],
                                            "attribution_setting": mapped_delivery_df["attribution_setting"],
                                            "campaign_type" : mapped_delivery_df["campaign_type"],
                                            "src_fb_offline_purchases" : mapped_delivery_df["src_fb_offline_purchases"],
                                            "src_post_engagement" : mapped_delivery_df["src_post_engagement"],
                                            "src_views" : mapped_delivery_df["src_views"],
                                            "src_link_clicks" : mapped_delivery_df["src_link_clicks"],
                                            "src_fb_mobile_purchase" : mapped_delivery_df["src_fb_mobile_purchase"],
                                            "src_total_actions" : mapped_delivery_df["src_total_actions"],
                                            "src_fb_pixel_add_to_cart" : mapped_delivery_df["src_fb_pixel_add_to_cart"],
                                            "src_new_messaging_connections" : mapped_delivery_df["src_new_messaging_connections"],
                                            "src_purchase" : mapped_delivery_df["src_purchase"],
                                            "src_purchase_value" : mapped_delivery_df["src_purchase_value"],
                                            "src_unique_clicks" : mapped_delivery_df["src_unique_clicks"],
                                            "src_frequency" : mapped_delivery_df["src_frequency"],
                                            "src_comments" : mapped_delivery_df["src_comments"],
                                            "src_imps" : mapped_delivery_df["src_imps"],
                                            "src_clicks" : mapped_delivery_df["src_clicks"],
                                            "src_link_clicks_actions" : mapped_delivery_df["src_link_clicks_actions"],
                                            "src_video_avg_time_watched_actions" : mapped_delivery_df["src_video_avg_time_watched_actions"],
                                            "src_reach" : mapped_delivery_df["src_reach"],
                                            "src_video_30_sec_watched_actions" : mapped_delivery_df["src_video_30_sec_watched_actions"],
                                            "src_shares" : mapped_delivery_df["src_shares"],
                                            "src_page_likes" : mapped_delivery_df["src_page_likes"],
                                            "src_video_view_3s" : mapped_delivery_df["src_video_view_3s"],
                                            "src_add_to_cart" : mapped_delivery_df["src_add_to_cart"],
                                            "src_initiate_checkout" : mapped_delivery_df["src_initiate_checkout"],
                                            "src_video_p95_watched_actions" : mapped_delivery_df["src_video_p95_watched_actions"],
                                            "src_inline_post_engagement" : mapped_delivery_df["src_inline_post_engagement"],
                                            "src_post_reactions" : mapped_delivery_df["src_post_reactions"],
                                            "src_spend" : mapped_delivery_df["src_spend"],
                                            "src_video_play_actions_view_value" : mapped_delivery_df["src_video_play_actions_view_value"],
                                            "src_all_clicks" : mapped_delivery_df["src_all_clicks"],
                                            "src_average_play_time_count" : mapped_delivery_df["src_average_play_time_count"],
                                            "src_unique_video_view" : mapped_delivery_df["src_unique_video_view"],
                                            "src_fb_pixel_purchase" : mapped_delivery_df["src_fb_pixel_purchase"],
                                            "src_daily_budget" : mapped_delivery_df["src_daily_budget"],
                                            "src_lifetime_budget" : mapped_delivery_df["src_lifetime_budget"],
                                            "src_lifetime_budget_with_offset" : mapped_delivery_df["src_lifetime_budget_with_offset"],
                                            "src_daily_budget_with_offset" : mapped_delivery_df["src_daily_budget_with_offset"],
                                            "src_add_payment_info" : mapped_delivery_df["src_add_payment_info"],
                                            "src_add_to_wishlist" : mapped_delivery_df["src_add_to_wishlist"],
                                            "src_app_use" : mapped_delivery_df["src_app_use"],
                                            "src_complete_registration" : mapped_delivery_df["src_complete_registration"],
                                            "src_fb_mobile_add_payment_info" : mapped_delivery_df["src_fb_mobile_add_payment_info"],
                                            "src_fb_mobile_complete_registration" : mapped_delivery_df["src_fb_mobile_complete_registration"],
                                            "src_fb_mobile_content_view" : mapped_delivery_df["src_fb_mobile_content_view"],
                                            "src_fb_offline_purchase_conv_value" : mapped_delivery_df["src_fb_offline_purchase_conv_value"],
                                            "src_landing_page_view" : mapped_delivery_df["src_landing_page_view"],
                                            "src_lead" : mapped_delivery_df["src_lead"],
                                            "src_leads" : mapped_delivery_df["src_leads"],
                                            "src_mobile_app_install" : mapped_delivery_df["src_mobile_app_install"],
                                            "src_search" : mapped_delivery_df["src_search"],
                                            "src_thru_play" : mapped_delivery_df["src_thru_play"],
                                            "src_video_p100_watched_actions" : mapped_delivery_df["src_video_p100_watched_actions"],
                                            "src_video_p25_watched_actions" : mapped_delivery_df["src_video_p25_watched_actions"],
                                            "src_video_p50_watched_actions" : mapped_delivery_df["src_video_p50_watched_actions"],
                                            "src_video_p75_watched_actions" : mapped_delivery_df["src_video_p75_watched_actions"],
                                            "src_view_content" : mapped_delivery_df["src_view_content"],
                                            "src_lead_grouped" : mapped_delivery_df["src_lead_grouped"],
                                            "src_event_responses" : mapped_delivery_df["src_event_responses"],
                                            "src_page_engagement" : mapped_delivery_df["src_page_engagement"],
                                            "src_unique_landing_page_view" : mapped_delivery_df["src_unique_landing_page_view"],
                                            "src_contact_total" : mapped_delivery_df["src_contact_total"],
                                            "src_donate_total" : mapped_delivery_df["src_donate_total"],
                                            "src_schedule_total" : mapped_delivery_df["src_schedule_total"],
                                            "src_start_trial_total" : mapped_delivery_df["src_start_trial_total"],
                                            "src_submit_application_total" : mapped_delivery_df["src_submit_application_total"],
                                            "src_subscribe_total" : mapped_delivery_df["src_subscribe_total"],
                                            "src_unique_chekin" : mapped_delivery_df["src_unique_chekin"],
                                            "src_unique_link_click" : mapped_delivery_df["src_unique_link_click"],
                                            "src_unique_page_engagement" : mapped_delivery_df["src_unique_page_engagement"],
                                            "src_unique_like" : mapped_delivery_df["src_unique_like"],
                                            "src_unique_comment" : mapped_delivery_df["src_unique_comment"],
                                            "src_unique_post_engagement" : mapped_delivery_df["src_unique_post_engagement"],
                                            "src_unique_post_reactions" : mapped_delivery_df["src_unique_post_reactions"],
                                            "src_unique_shares" : mapped_delivery_df["src_unique_shares"],
                                            "date_wid" : mapped_delivery_df["date_wid"],
                                            "data_as_of_ts" : mapped_delivery_df["data_as_of_ts"],
                                            "file_name": mapped_delivery_df["file_name"],
                                            "file_row_number": mapped_delivery_df["file_row_number"],
                                            "created_batch_id" : mapped_delivery_df["created_batch_id"],
                                            "created_ts" : mapped_delivery_df["created_ts"],
                                            "modified_batch_id" : mapped_delivery_df["modified_batch_id"],
                                            "modified_ts" : mapped_delivery_df["modified_ts"]})])

    def get_all_metrics(self, df, batch_id):
        '''gets supporting metrics'''
        final_df = (df
                    .withColumnRenamed("external_adset_id", "meta_ad_set_id")
                    .withColumnRenamed("external_campaign_id", "meta_campaign_id")
                    .withColumn("meta_ad_set_wid", F.sha1(F.concat(F.col("integration_source_code"),
                                                                     F.col("agency_id"),
                                                                     F.col("meta_ad_set_id"),
                                                                     F.col("meta_campaign_id"))))
                    .withColumn("meta_campaign_wid", F.sha1(F.concat(F.col("integration_source_code"),
                                                                     F.col("agency_id"),
                                                                     F.col("agency_id"),
                                                                     F.col("meta_campaign_id"))))
                    .withColumn("delivery_wid", F.sha1(F.concat(F.col("integration_source_code"),
                                                                F.col("agency_id"),
                                                                F.col("line_item_lineage_id"),
                                                                F.col("delivery_data_representation_id"),
                                                                F.col("meta_ad_set_id"),
                                                                F.col("meta_campaign_id"),
                                                                F.col("delivered_on_date"))))
                    .withColumn("date_wid", F.to_char("delivered_on_date", 'YYYYMMDD').cast(IntegerType()))
                    .withColumn("created_batch_id", F.lit(batch_id))
                    .withColumn("created_ts", F.current_timestamp())
                    .withColumn("modified_batch_id", F.lit(batch_id))
                    .withColumn("modified_ts", F.current_timestamp())
                    .withColumnRenamed("file_created_ts", "data_as_of_ts")
                    .withColumnRenamed("external_account_id", "meta_account_id")
                    .select(all_col_names))
        return final_df

    def get_raw_delivery_w_src_col(self, raw_delivery_df):
        '''removes meta data cols'''
        raw_delivery_mod_df = (raw_delivery_df
                               .drop("created_batch_id", "created_ts", "modified_batch_id", "modified_ts"))

        all_raw_del_col_names = [x.lower() for x in raw_delivery_mod_df.columns]
        columns_to_rename_list = [x for x in all_raw_del_col_names if x not in ['integration_source_code',
                                                                                'agency_id',
                                                                                'external_campaign_id',
                                                                                'external_adset_id',
                                                                                'delivered_on_date',
                                                                                "external_account_id",
                                                                                "file_name",
                                                                                "file_row_number",
                                                                                "file_created_ts",
                                                                                "record_content_md5",
                                                                                "attribution_setting",
                                                                                "campaign_type"
                                                                                ]]
        raw_delivery_w_prefix_df = self.handle_nulls_number_and_strings_cols(self.add_src_prefix(raw_delivery_mod_df,
                                                       columns_to_rename_list))
        return raw_delivery_w_prefix_df

    def process_incr_raw_delivery(self, session, logger, target, get_all_metrics_fn, integration_source_code):
        '''get incremental deliveries'''
        var_raw_delivery_stream    = "meta_lineitem_ad_set_daily_msg_mcl_stream"
        var_line_item_tracker_name = "brg_delivery_mapping_third_party"

        raw_delivery_stream = (f"""{get_dict_element(self.config, "db_details.third_party.db_name")}."""
                            f"""{get_dict_element(self.config, "db_details.third_party.schema_name")}."""
                            f"""{var_raw_delivery_stream}""")

        incr_raw_delivery_df = (session.sql("select * from {0}".format(raw_delivery_stream))
                                .filter(F.col("METADATA$ACTION") == 'INSERT'))

        incr_raw_delivery_w_prefix_df = self.get_raw_delivery_w_src_col(incr_raw_delivery_df)

        line_item_tracker_name = (f"""{get_dict_element(self.config, "db_details.basis_platform.db_name")}.
                                  {get_dict_element(self.config, "db_details.basis_platform.schema_name")}.
                                  {var_line_item_tracker_name}""")

        line_item_tracker_df = (session.sql("select * from {0}".format(line_item_tracker_name))
                                .withColumnRenamed("agency_id", "li_agency_id")
                                .select("line_item_lineage_id",
                                        "line_item_start_date",
                                        "line_item_end_date",
                                        "delivery_data_representation_id",
                                        "delivery_data_mapping_external_id",
                                        "li_agency_id")
                                .filter(F.col("integration_source_code") == F.lit(integration_source_code))
                                )

        mapped_delivery_df = (
            line_item_tracker_df.join(incr_raw_delivery_w_prefix_df,
                                      (line_item_tracker_df["delivery_data_mapping_external_id"] ==
                                       incr_raw_delivery_w_prefix_df["external_campaign_id"]) &
                                      (line_item_tracker_df["li_agency_id"] == incr_raw_delivery_w_prefix_df["agency_id"]))
                                .filter(F.col("delivered_on_date").between(F.col("line_item_start_date"),
                                                                           F.col("line_item_end_date"))))

        final_incr_raw_delivery_df = (get_all_metrics_fn(mapped_delivery_df)
                                      .sort("date_wid",
                                            "line_item_lineage_id",
                                            "delivery_data_representation_id"))

        self.merge_data(target, final_incr_raw_delivery_df)

    def save_data(self, session, logger, batch_id):
        logger.info("Merging data to Target Table")

        # declare object names variables
        var_line_item_tracker_name  = "meta_lineitem_ad_set_daily_lm_stream"
        var_target_table_name       = "fct_delivery_meta_lineitem_adset_daily_mapped"
        var_raw_delivery_table_name = "msg_delivery_meta_ad_set_latest"
        integration_source_code     = "facebook_ad_server"
        var_reload_stream_name      = "meta_lineitem_ad_set_daily_rid_stream"

        line_item_tracker_name = (f"""{get_dict_element(self.config, "db_details.third_party.db_name")}."""
                                  f"""{get_dict_element(self.config, "db_details.third_party.schema_name")}."""
                                  f"""{var_line_item_tracker_name}""")

        li_changes_df = (session.sql(f"select * from {line_item_tracker_name}")
                         .filter(F.col("integration_source_code") == F.lit(integration_source_code))
                         .drop("integration_source_code"))

        target_table_name = (f"""{get_dict_element(self.config, "db_details.third_party.db_name")}."""
                            f"""{get_dict_element(self.config, "db_details.third_party.schema_name")}."""
                            f"""{var_target_table_name}""")

        target = session.table(target_table_name)

        raw_delivery_table_name = (f"""{get_dict_element(self.config, "db_details.integration.db_name")}."""
                                f"""{get_dict_element(self.config, "db_details.integration.third_party_schema_name")}."""
                                f"""{var_raw_delivery_table_name}""")

        raw_delivery_df = session.sql("select * from {0}".format(raw_delivery_table_name))

        raw_delivery_w_prefix_df = self.get_raw_delivery_w_src_col(raw_delivery_df)

        # # add defaults for src_ columns
        raw_delivery_w_prefix_df = self.handle_nulls_number_and_strings_cols(raw_delivery_w_prefix_df)

        get_all_metrics_fn = partial(self.get_all_metrics, batch_id=batch_id)

        self.process_insert_event(li_changes_df,
                                  raw_delivery_w_prefix_df,
                                  target,
                                  target_table_name,
                                  get_all_metrics_fn)

        self.process_update_event(li_changes_df,
                                  raw_delivery_w_prefix_df,
                                  target,
                                  target_table_name,
                                  get_all_metrics_fn)

        self.process_delete_event(li_changes_df,
                                  target)

        self.process_incr_raw_delivery(session,
                                       logger,
                                       target,
                                       get_all_metrics_fn,
                                       integration_source_code)

        reload_stream_name = ("{0}.{1}.{2}".format(get_dict_element(self.config, "db_details.third_party.db_name"),
                                      get_dict_element(self.config, "db_details.third_party.schema_name"),
                                      var_reload_stream_name))

        self.reload(session,
                    logger,
                    raw_delivery_w_prefix_df,
                    get_all_metrics_fn,
                    target,
                    var_target_table_name,
                    target_table_name,
                    reload_stream_name,
                    integration_source_code)
