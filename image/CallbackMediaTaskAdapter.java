package com.tommi.media.process.adapter.impl.notice;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.tommi.media.process.adapter.NoticeMediaTaskAdapter;
import com.tommi.media.process.entity.po.TommiMediaTaskPO;
import com.tommi.media.process.common.enums.CommonMediaTaskStatusEnums;
import com.tommi.media.process.service.TommiMediaTaskService;
import com.tommi.pet.api.common.process.dto.TommiMediaTaskRepDTO;
import com.tommi.pet.api.common.process.dto.way.CallBackNoticeDTO;
import com.tommi.pet.common.core.constant.SecurityConstants;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpStatus;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;

import org.dromara.dynamictp.core.DtpRegistry;
import org.dromara.dynamictp.core.thread.DtpExecutor;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * rmi回调方式-结果通知处理器
 * @author y
 * @create 2023/6/20 11:15
 */
@Service
@Slf4j
public class CallbackMediaTaskAdapter implements NoticeMediaTaskAdapter, InitializingBean {

	@Autowired
	private TommiMediaTaskService tommiMediaTaskService;

	private final DtpExecutor mediaRpcDtpExecutor = DtpRegistry.getDtpExecutor("media-rpc-dtp");


	/**
	 * 初始化后,重试之前的
	 * @throws Exception
	 */
	@Override
	public void afterPropertiesSet(){

		//扫描预终状态的任务
		LambdaQueryWrapper<TommiMediaTaskPO> lambdaQueryWrapper = new LambdaQueryWrapper<>();
		lambdaQueryWrapper.ge(TommiMediaTaskPO::getTaskStatus,2000);
		lambdaQueryWrapper.lt(TommiMediaTaskPO::getTaskStatus,3000);
		List<TommiMediaTaskPO> list = tommiMediaTaskService.list(lambdaQueryWrapper);

		if(ObjectUtil.isEmpty(list) && !list.isEmpty()){
			for (TommiMediaTaskPO mediaTask : list) {
				this.notice(mediaTask,mediaTask.getTaskStatus().equals(CommonMediaTaskStatusEnums.TASK_DONE.getCode()));
			}
		}

	}

	/**
	 * 通知
	 * @param commonMediaTask
	 * @param success
	 */
	@Override
	public void notice(TommiMediaTaskPO commonMediaTask, Boolean success) {

		mediaRpcDtpExecutor.execute(()->{
			try {
				//todo 检验是否有效

				//包装任务
				Long bizId = commonMediaTask.getBizId();
				JSONObject resultData = commonMediaTask.getResultData();
				JSONObject noticeMode = commonMediaTask.getNoticeMode();
				CallBackNoticeDTO taskNoticeDTO = JSONObject.toJavaObject(noticeMode,CallBackNoticeDTO.class);
				TommiMediaTaskRepDTO  mediaTaskRepDTO = new TommiMediaTaskRepDTO();
				mediaTaskRepDTO.setBizId(bizId);
				mediaTaskRepDTO.setResultData(resultData);
				mediaTaskRepDTO.setSuccess(success);

				//包装请求url
				String finalReturnUrl = taskNoticeDTO.getDomain() + taskNoticeDTO.getReturnPath();

				//todo 看是否需要鉴权
				log.info(finalReturnUrl);

				//发送请求
				HttpResponse response = HttpRequest.post(finalReturnUrl)
						.header(SecurityConstants.FROM, SecurityConstants.FROM_IN)
						.timeout(2000)
						.body(JSONUtil.toJsonStr(mediaTaskRepDTO))
						.execute();

				//请求成功200
				if(response.getStatus()== HttpStatus.HTTP_OK){

					if(success){
						tommiMediaTaskService.changeStatus(commonMediaTask, CommonMediaTaskStatusEnums.TASK_CALLBACK_NOTICE_SUCCESS_DONE);
					}else {
						tommiMediaTaskService.changeStatus(commonMediaTask, CommonMediaTaskStatusEnums.TASK_CALLBACK_NOTICE_FAIL_DONE);
					}

				//500,404等其他状态码
				}else {

					tommiMediaTaskService.changeStatus(commonMediaTask, CommonMediaTaskStatusEnums.TASK_CALLBACK_RESULT_SEND_WARN);

					JSONObject referenceData = commonMediaTask.getReferenceData();
					if(referenceData.containsKey("callBackErrorTime")){
						referenceData.put("callBackErrorTime",referenceData.getIntValue("callBackErrorTime")+1);
						log.info(commonMediaTask.getId()+"第"+referenceData.getIntValue("callBackErrorTime")+"次失败");
						//已经请求10次
						if(referenceData.getIntValue("callBackErrorTime")>10){
							tommiMediaTaskService.changeStatus(commonMediaTask, CommonMediaTaskStatusEnums.TASK_CALLBACK_RESULT_SEND_EXHAUST);
							commonMediaTask.setReferenceData(referenceData);
						//请求不足10次
						}else {
							commonMediaTask.setReferenceData(referenceData);
							//放入下一次请求中
							this.notice(commonMediaTask,success);
						}
						tommiMediaTaskService.updateById(commonMediaTask);
					}else {
						referenceData.put("callBackErrorTime",1);
						commonMediaTask.setReferenceData(referenceData);
						tommiMediaTaskService.updateById(commonMediaTask);
					}
				}
			} catch (Exception e) {
				tommiMediaTaskService.changeStatus(commonMediaTask, CommonMediaTaskStatusEnums.TASK_CALLBACK_RESULT_SEND_ERROR);
			}
		});
	}



}
