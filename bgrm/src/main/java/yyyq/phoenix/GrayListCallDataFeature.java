/**
 * binguolife.com Inc.
 * Copyright (c) 2013-2017 All Rights Reserved.
 */
package yyyq.phoenix;


/**
 * 催收名单互通特征 
 * 
 * @author qiang.wq
 * @version $Id: GrayListCallDataFeature.java, v 0.1 2017年9月27日 上午12:42:32 qiang.wq Exp $
 */
public class GrayListCallDataFeature{

    private String ID;          //  clientNo|creditNo
    private String number;      //  匹配催收库号码的个数
    private String frequency;   //  匹配催收库号码各次数

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public String getNumber() {
        return number;
    }

    public void setNumber(String number) {
        this.number = number;
    }

    public String getFrequency() {
        return frequency;
    }

    public void setFrequency(String frequency) {
        this.frequency = frequency;
    }
}
