package com.run.ayena.objectpp.util;

import org.apache.hadoop.util.ProgramDriver;

import com.run.ayena.objectpp.mr.ObjectMergeMR;

/**
 * @author Yanhong Lee
 * 
 */
public class HadoopAppDriver {

	public static int exec(String[] args) {
		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("object.merge", ObjectMergeMR.class, "对象归并MR");

			pgd.driver(args);

			// Success
			exitCode = 0;
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return exitCode;
	}

	public static void main(String[] args) {
		int exitCode = exec(args);
		System.exit(exitCode);
	}

}
