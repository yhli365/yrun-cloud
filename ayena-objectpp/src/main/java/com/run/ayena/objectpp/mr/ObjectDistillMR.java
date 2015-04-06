package com.run.ayena.objectpp.mr;

import com.run.ayena.objectpp.util.HadoopTool;

/**
 * 对象提取作业.
 * 
 * @author Yanhong Lee
 * 
 */
public class ObjectDistillMR extends HadoopTool {
	
	public static void main(String[] args) throws Exception {
		execMain(new ObjectDistillMR(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
