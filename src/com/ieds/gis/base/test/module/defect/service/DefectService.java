package com.ieds.gis.base.test.module.defect.service;

import java.util.ArrayList;
import java.util.List;

import android.app.Activity;
import android.content.Intent;

import com.ieds.gis.base.dialog.MyToast;
import com.ieds.gis.base.edit.EditFactoryActivity;
import com.ieds.gis.base.edit.bo.EditBo;
import com.ieds.gis.base.test.dao.SqliteDAO;
import com.ieds.gis.base.test.module.defect.page.DefectActivity;
import com.ieds.gis.base.test.page.TestMainActivity;
import com.ieds.gis.base.test.po.CHK_DIS_DEFECT;
import com.ieds.gis.base.test.service.IEditorService;
import com.ieds.gis.base.widget.bo.AbsWidgetBo;
import com.ieds.gis.base.widget.bo.FieldValue;
import com.ieds.gis.base.widget.bo.WidgetSpinnerBo;

public class DefectService implements IEditorService {

	private Activity act;

	public DefectService(Activity act) {
		super();
		this.act = act;
	}

	/**
	 * @param args
	 */
	public void openEditPage(String id) {
		CHK_DIS_DEFECT c = new CHK_DIS_DEFECT();
		c.setStatus("1");
		c.setId(id);
		try {
			List<FieldValue> listValue = new ArrayList<FieldValue>();
			FieldValue fv1 = new FieldValue("流量1", "sdf");
			FieldValue fv2 = new FieldValue("流量2", "sdf");
			listValue.add(fv1);
			listValue.add(fv2);
			List<AbsWidgetBo> addList = new ArrayList<AbsWidgetBo>();
			WidgetSpinnerBo wsb3 = new WidgetSpinnerBo(
					CHK_DIS_DEFECT.class.getSimpleName() + "1", true, "测试",
					"equip_name", true, listValue);
			addList.add(wsb3);
			EditBo eb = new EditBo(c, true, addList);
			Intent i = EditFactoryActivity.getEditIntent(act,
					DefectActivity.class, eb);
			EditFactoryActivity.toEditActivity(act, i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MyToast.showToast(e.getMessage());
		}
	}

	public void openBrowsePage(String id) {
		CHK_DIS_DEFECT c = new CHK_DIS_DEFECT();
		c.setId(id);
		try {
			c = SqliteDAO.getInstance().findFirst(c);

			EditBo eb = new EditBo(c, false);
			Intent i = EditFactoryActivity.getEditIntent(act,
					DefectActivity.class, eb);
			EditFactoryActivity.toEditActivity(act, i);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			MyToast.showToast(e.getMessage());
		}
	}
}
