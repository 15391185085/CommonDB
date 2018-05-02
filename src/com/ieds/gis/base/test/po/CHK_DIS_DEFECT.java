package com.ieds.gis.base.test.po;

import java.io.Serializable;
import java.util.Date;
import javax.persistence.Transient;
import javax.persistence.Id;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "CHK_DIS_DEFECT")
public class CHK_DIS_DEFECT implements Serializable {
	@Id
	private String id;

	public void setId(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	private String defect_code;

	public void setDefect_code(String defect_code) {
		this.defect_code = defect_code;
	}

	public String getDefect_code() {
		return defect_code;
	}

	private String channel;

	public void setChannel(String channel) {
		this.channel = channel;
	}

	public String getChannel() {
		return channel;
	}

	@Transient
	private String task_id;

	public void setTask_id(String task_id) {
		this.task_id = task_id;
	}

	public String getTask_id() {
		return task_id;
	}

	private String line_id;

	public void setLine_id(String line_id) {
		this.line_id = line_id;
	}

	public String getLine_id() {
		return line_id;
	}

	private String line_name;

	public void setLine_name(String line_name) {
		this.line_name = line_name;
	}

	public String getLine_name() {
		return line_name;
	}

	private String secl_id;

	public void setSecl_id(String secl_id) {
		this.secl_id = secl_id;
	}

	public String getSecl_id() {
		return secl_id;
	}

	private String secl_name;

	public void setSecl_name(String secl_name) {
		this.secl_name = secl_name;
	}

	public String getSecl_name() {
		return secl_name;
	}

	private String voltagelevel;

	public void setVoltagelevel(String voltagelevel) {
		this.voltagelevel = voltagelevel;
	}

	public String getVoltagelevel() {
		return voltagelevel;
	}

	private String equip_id;

	public void setEquip_id(String equip_id) {
		this.equip_id = equip_id;
	}

	public String getEquip_id() {
		return equip_id;
	}

	private String equip_name;

	public void setEquip_name(String equip_name) {
		this.equip_name = equip_name;
	}

	public String getEquip_name() {
		return equip_name;
	}

	private String equip_category_id;

	public void setEquip_category_id(String equip_category_id) {
		this.equip_category_id = equip_category_id;
	}

	public String getEquip_category_id() {
		return equip_category_id;
	}

	private String parts_id;

	public void setParts_id(String parts_id) {
		this.parts_id = parts_id;
	}

	public String getParts_id() {
		return parts_id;
	}

	private String place_id;

	public void setPlace_id(String place_id) {
		this.place_id = place_id;
	}

	public String getPlace_id() {
		return place_id;
	}

	private String description_id;

	public void setDescription_id(String description_id) {
		this.description_id = description_id;
	}

	public String getDescription_id() {
		return description_id;
	}

	private String classification_id;

	public void setClassification_id(String classification_id) {
		this.classification_id = classification_id;
	}

	public String getClassification_id() {
		return classification_id;
	}

	private String defect_level;

	public void setDefect_level(String defect_level) {
		this.defect_level = defect_level;
	}

	public String getDefect_level() {
		return defect_level;
	}

	private String content;

	public void setContent(String content) {
		this.content = content;
	}

	public String getContent() {
		return content;
	}

	private String defect_user_ids;

	public void setDefect_user_ids(String defect_user_ids) {
		this.defect_user_ids = defect_user_ids;
	}

	public String getDefect_user_ids() {
		return defect_user_ids;
	}

	private Date defect_time;

	public void setDefect_time(Date defect_time) {
		this.defect_time = defect_time;
	}

	public Date getDefect_time() {
		return defect_time;
	}

	private String mobile_serial;

	public void setMobile_serial(String mobile_serial) {
		this.mobile_serial = mobile_serial;
	}

	public String getMobile_serial() {
		return mobile_serial;
	}

	private String status;

	public void setStatus(String status) {
		this.status = status;
	}

	public String getStatus() {
		return status;
	}

	private String is_report;

	public void setIs_report(String is_report) {
		this.is_report = is_report;
	}

	public String getIs_report() {
		return is_report;
	}

	private String weather;

	public void setWeather(String weather) {
		this.weather = weather;
	}

	public String getWeather() {
		return weather;
	}

	private String report_user_id;

	public void setReport_user_id(String report_user_id) {
		this.report_user_id = report_user_id;
	}

	public String getReport_user_id() {
		return report_user_id;
	}

	private Date report_time;

	public void setReport_time(Date report_time) {
		this.report_time = report_time;
	}

	public Date getReport_time() {
		return report_time;
	}

	private String report_content;

	public void setReport_content(String report_content) {
		this.report_content = report_content;
	}

	public String getReport_content() {
		return report_content;
	}

	private String deal_group_id;

	public void setDeal_group_id(String deal_group_id) {
		this.deal_group_id = deal_group_id;
	}

	public String getDeal_group_id() {
		return deal_group_id;
	}

	private String del_flag;

	public void setDel_flag(String del_flag) {
		this.del_flag = del_flag;
	}

	public String getDel_flag() {
		return del_flag;
	}

	private Date create_time;

	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}

	public Date getCreate_time() {
		return create_time;
	}

	private String create_user_id;

	public void setCreate_user_id(String create_user_id) {
		this.create_user_id = create_user_id;
	}

	public String getCreate_user_id() {
		return create_user_id;
	}

	private String create_group_id;

	public void setCreate_group_id(String create_group_id) {
		this.create_group_id = create_group_id;
	}

	public String getCreate_group_id() {
		return create_group_id;
	}

	private String create_depart_id;

	public void setCreate_depart_id(String create_depart_id) {
		this.create_depart_id = create_depart_id;
	}

	public String getCreate_depart_id() {
		return create_depart_id;
	}

	private String create_organ_id;

	public void setCreate_organ_id(String create_organ_id) {
		this.create_organ_id = create_organ_id;
	}

	public String getCreate_organ_id() {
		return create_organ_id;
	}

	private String create_user_name;

	public void setCreate_user_name(String create_user_name) {
		this.create_user_name = create_user_name;
	}

	public String getCreate_user_name() {
		return create_user_name;
	}

	private String defect_user_names;

	public void setDefect_user_names(String defect_user_names) {
		this.defect_user_names = defect_user_names;
	}

	public String getDefect_user_names() {
		return defect_user_names;
	}

	@Transient
	private Integer uploaded;

	public void setUploaded(Integer uploaded) {
		this.uploaded = uploaded;
	}

	public Integer getUploaded() {
		return uploaded;
	}

	public String handle_status;

	public String getHandle_status() {
		return handle_status;
	}

	public void setHandle_status(String handle_status) {
		this.handle_status = handle_status;
	}

	public CHK_DIS_DEFECT() {
		super();
	}
}
