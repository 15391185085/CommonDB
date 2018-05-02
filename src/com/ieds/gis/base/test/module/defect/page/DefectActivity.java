package com.ieds.gis.base.test.module.defect.page;

import android.view.View;
import android.view.View.OnClickListener;

import com.ieds.gis.base.dao.DbUtils;
import com.ieds.gis.base.dialog.MyToast;
import com.ieds.gis.base.edit.EditFactoryActivity;
import com.ieds.gis.base.test.dao.SqliteDAO;
import com.lidroid.xutils.exception.DbException;

public class DefectActivity extends EditFactoryActivity {

	@Override
	public DbUtils getDb() throws DbException {
		// TODO Auto-generated method stub
		return SqliteDAO.getInstance();
	}

	@Override
	public void saveOk() throws Exception {
		// TODO Auto-generated method stub
		MyToast.showToast("saveOk()");
	}

	@Override
	public void saveCancel() throws Exception {
		// TODO Auto-generated method stub
		MyToast.showToast("saveCancel()");
	}

	@Override
	public void initView() throws DbException {
		// TODO Auto-generated method stub
		super.initView();
		initTextView("测试标题", "测试内容");
		initButton1("新增", new OnClickListener() {

			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				MyToast.showToast("1");
			}
		});
		initButton2("删除", new OnClickListener() {

			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				MyToast.showToast("2");
			}
		});
		initButton3("编辑", new OnClickListener() {

			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				MyToast.showToast("3");
			}
		});
		initButton4("上传", new OnClickListener() {

			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				MyToast.showToast("4");
			}
		});
		initButton5("下载", new OnClickListener() {

			@Override
			public void onClick(View v) {
				// TODO Auto-generated method stub
				MyToast.showToast("5");
			}
		});
	}

}
