package org.apache.commons.dbutils;

import java.util.ArrayList;
import java.util.List;
/**
 * 分页bean 通过getList()获得分页列表
 * Pagin<Map<String,Object> page=new Pagin(int pageNumber,int rowsCount,int pageMaxRow,List<T> list);
 * Note:页码从0开始
 * 2:57:56 PM
 */
public class Pagin<T> {
	private int pageMaxRows = 10;// 每页显示记录
	private int totalRows = 0;// 总行数
	private int totalPages = 0;// 总页数
	private long times=0l;//查询时间
	/**当前页*/
	private int currentPage = 1;// 当前页
	private List<T> rows;// 传过一个list，就可以对list进行分页
	private boolean nextPage = false;// 没有下一页
	private boolean previousPage = false;// 没有上一页
//	/**
//	 * 指针构造方法
//	 * @param start 当前页开始数
//	 * @param allowMaxRow 当前页允许的最大记录
//	 * @param list 当前页数据
//	 * @param totalRow 系统总数
//	 */
//	public Pagin(int startRow,int allowMaxRow,List<T> list,int totalRow){
//		this.rows=list;
//		this.totalRows=totalRow;
//		this.pageMaxRows=allowMaxRow<0?0:allowMaxRow;
//		this.totalPages=(int)Math.ceil(Double.valueOf(this.totalRows)/this.pageMaxRows);
//		this.currentPage=this.totalRow/start;
//	}
	public Pagin(int pageNumber,int pageMaxRow){
		this.rows=new ArrayList(0);
		this.totalRows=0;
		this.currentPage=pageNumber<1?1:pageNumber;
		this.pageMaxRows=pageMaxRow<0?0:pageMaxRow;
		this.totalPages=(int)Math.ceil(Double.valueOf(this.totalRows)/this.pageMaxRows);
		this.nextPage=this.currentPage<this.totalPages;
		this.previousPage=pageNumber>1;
	}
	/**
	 * 页码构造方法
	 * @param pageNumber 当前页码 
	 * @param rowsCount 系统查询后得到的结果总记录数
	 * @param pageMaxRow 每页显示总数
	 * @param list 系统查询后获得到的结果
	 */
	public Pagin(int pageNumber,int rowsCount,int pageMaxRow,List<T> list){
		this.rows=list;
		this.totalRows=rowsCount<0?0:rowsCount;
		this.currentPage=pageNumber<1?1:pageNumber;
		this.pageMaxRows=pageMaxRow<0?0:pageMaxRow;
		this.totalPages=(int)Math.ceil(Double.valueOf(this.totalRows)/this.pageMaxRows);
		this.nextPage=this.currentPage<this.totalPages;
		this.previousPage=pageNumber>1;
	}
	public void setTimes(long times) {
		this.times = times;
	}
	public int getPageMaxRows() {
		return pageMaxRows;
	}
	public void setPageMaxRows(int pageMaxRows) {
		this.pageMaxRows = pageMaxRows;
	}
	public int getTotalRows() {
		return totalRows;
	}
	public void setTotalRows(int totalRows) {
		this.totalRows = totalRows;
	}
	public int getTotalPages() {
		return totalPages;
	}
	public void setTotalPages(int totalPages) {
		this.totalPages = totalPages;
	}
	public int getCurrentPage() {
		return currentPage;
	}
	public void setCurrentPage(int currentPage) {
		this.currentPage = currentPage;
	}
	public List<T> getRows() {
		return rows;
	}
	public void setRows(List<T> rows) {
		this.rows = rows;
	}
	public boolean isNextPage() {
		return nextPage;
	}
	public void setNextPage(boolean nextPage) {
		this.nextPage = nextPage;
	}
	public boolean isPreviousPage() {
		return previousPage;
	}
	public void setPreviousPage(boolean previousPage) {
		this.previousPage = previousPage;
	}
	public static void main(String[]args){
		int i=(int)Math.ceil(Double.valueOf(0)/20);
		System.out.println(i);
	}
	public long getTimes() {
		return times;
	}
}
