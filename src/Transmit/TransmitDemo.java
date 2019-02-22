package Transmit;

import java.util.Scanner;

import net.sf.json.JSONObject;
import ClassDemo.Lora;
import ClassDemo.TPDemo;

public class TransmitDemo {
	public static void main(String[] args) {

		int id = (int) (Math.random() * 10);
		int minute = 0;
		System.out.println(Dispose.DisposeMapReduce.averageDemo
		);
		
		System.out.println("请输入触发值:");
		Scanner input = new Scanner (System.in);
		int discreetValue = input.nextInt();
		if (Dispose.DisposeMapReduce.averageDemo > discreetValue) {
			minute = 5;
		}
		TPDemo tpDemo = new TPDemo();
		tpDemo.setId(id);
		tpDemo.setMimute(minute);
		JSONObject json = JSONObject.fromObject(tpDemo);
		System.out.println(json);
		Lora lora = new Lora();
		lora.setId(1);
		lora.setTp(10);

		json = JSONObject.fromObject(lora);
		System.out.print(json);

	}
}