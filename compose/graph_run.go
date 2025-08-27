/*
 * Copyright 2024 CloudWeGo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package compose

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/cloudwego/eino/internal"
)

type chanCall struct {
	action          *composableRunnable
	writeTo         []string
	writeToBranches []*GraphBranch

	controls []string // branch must control

	preProcessor, postProcessor *composableRunnable
}

type chanBuilder func(dependencies []string, indirectDependencies []string, zeroValue func() any, emptyStream func() streamReader) channel

type runner struct {
	chanSubscribeTo map[string]*chanCall

	successors          map[string][]string
	dataPredecessors    map[string][]string
	controlPredecessors map[string][]string

	inputChannels *chanCall

	chanBuilder chanBuilder // could be nil
	eager       bool
	dag         bool

	runCtx func(ctx context.Context) context.Context

	options graphCompileOptions

	inputType  reflect.Type
	outputType reflect.Type

	// take effect as a subgraph through toComposableRunnable
	inputStreamFilter                               streamMapFilter
	inputConverter                                  handlerPair
	inputFieldMappingConverter                      handlerPair
	inputConvertStreamPair, outputConvertStreamPair streamConvertPair

	*genericHelper

	// checks need to do because cannot check at compile
	runtimeCheckEdges    map[string]map[string]bool
	runtimeCheckBranches map[string][]bool

	edgeHandlerManager      *edgeHandlerManager
	preNodeHandlerManager   *preNodeHandlerManager
	preBranchHandlerManager *preBranchHandlerManager

	checkPointer         *checkPointer
	interruptBeforeNodes []string
	interruptAfterNodes  []string

	mergeConfigs map[string]FanInMergeConfig
}

func (r *runner) invoke(ctx context.Context, input any, opts ...Option) (any, error) {
	return r.run(ctx, false, input, opts...)
}

func (r *runner) transform(ctx context.Context, input streamReader, opts ...Option) (streamReader, error) {
	s, err := r.run(ctx, true, input, opts...)
	if err != nil {
		return nil, err
	}

	return s.(streamReader), nil
}

type runnableCallWrapper func(context.Context, *composableRunnable, any, ...any) (any, error)

func runnableInvoke(ctx context.Context, r *composableRunnable, input any, opts ...any) (any, error) {
	return r.i(ctx, input, opts...)
}

func runnableTransform(ctx context.Context, r *composableRunnable, input any, opts ...any) (any, error) {
	return r.t(ctx, input.(streamReader), opts...)
}

/**
 * 执行图的核心方法，支持同步调用和流式处理两种模式
 *
 * @param ctx 上下文信息，用于控制执行过程和传递状态
 * @param isStream 是否为流式处理模式，决定使用invoke还是transform包装器
 * @param input 图的输入数据，可以是任意类型
 * @param opts 执行选项，包含检查点、状态修改器等配置
 * @return result 图执行的最终结果
 * @return err 执行过程中的错误信息
 */
func (r *runner) run(ctx context.Context, isStream bool, input any, opts ...Option) (result any, err error) {
	/*
	 * 根据执行模式选择合适的包装函数
	 * 流式模式使用transform包装器，同步模式使用invoke包装器
	 */
	haveOnStart := false
	defer func() {
		if !haveOnStart {
			ctx, input = onGraphStart(ctx, input, isStream)
		}
		if err != nil {
			ctx, err = onGraphError(ctx, err)
		} else {
			ctx, result = onGraphEnd(ctx, result, isStream)
		}
	}()
	var runWrapper runnableCallWrapper
	runWrapper = runnableInvoke
	if isStream {
		runWrapper = runnableTransform
	}

	/*
	 * 初始化通道管理器和任务管理器
	 * 通道管理器负责节点间的数据流转，任务管理器负责并发执行控制
	 */
	cm := r.initChannelManager(isStream)
	tm := r.initTaskManager(runWrapper, opts...)
	maxSteps := r.options.maxRunSteps

	/*
	 * 处理最大执行步数限制
	 * DAG模式不支持步数限制，普通模式需要验证步数有效性
	 */
	if r.dag {
		for i := range opts {
			if opts[i].maxRunSteps > 0 {
				return nil, newGraphRunError(fmt.Errorf("cannot set max run steps in dag"))
			}
		}
	} else {
		/* 从选项中更新最大步数配置 */
		for i := range opts {
			if opts[i].maxRunSteps > 0 {
				maxSteps = opts[i].maxRunSteps
			}
		}
		if maxSteps < 1 {
			return nil, newGraphRunError(errors.New("max run steps limit must be at least 1"))
		}
	}

	/*
	 * 提取并验证每个节点的执行选项
	 * 选项映射用于在执行时为特定节点提供定制化参数
	 */
	optMap, extractErr := extractOption(r.chanSubscribeTo, opts...)
	if extractErr != nil {
		return nil, newGraphRunError(fmt.Errorf("graph extract option fail: %w", extractErr))
	}

	/*
	 * 提取检查点相关信息
	 * 包括检查点ID、写入目标、状态修改器和强制新运行标志
	 */
	checkPointID, writeToCheckPointID, stateModifier, forceNewRun := getCheckPointInfo(opts...)
	if checkPointID != nil && r.checkPointer.store == nil {
		return nil, newGraphRunError(fmt.Errorf("receive checkpoint id but have not set checkpoint store"))
	}

	/* 提取子图信息，用于判断当前执行上下文 */
	path, isSubGraph := getNodeKey(ctx)

	/*
	 * 从检查点或初始化图状态
	 * 支持从上下文或存储中加载检查点进行状态恢复
	 */
	initialized := false
	var nextTasks []*task
	if cp := getCheckPointFromCtx(ctx); cp != nil {
		// in subgraph, try to load checkpoint from ctx
		// load checkpoint from ctx
		initialized = true // don't init again

		err = r.checkPointer.restoreCheckPoint(cp, isStream)
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("restore checkpoint fail: %w", err))
		}

		err = cm.loadChannels(cp.Channels)
		if err != nil {
			return nil, newGraphRunError(err)
		}
		if sm := getStateModifier(ctx); sm != nil && cp.State != nil {
			err = sm(ctx, *path, cp.State)
			if err != nil {
				return nil, newGraphRunError(fmt.Errorf("state modifier fail: %w", err))
			}
		}
		if cp.State != nil {
			ctx = context.WithValue(ctx, stateKey{}, &internalState{state: cp.State})
		}

		ctx, input = onGraphStart(ctx, input, isStream)
		haveOnStart = true
		nextTasks, err = r.restoreTasks(ctx, cp.Inputs, cp.SkipPreHandler, cp.ToolsNodeExecutedTools, cp.RerunNodes, isStream, optMap) // should restore after set state to context
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("restore tasks fail: %w", err))
		}
	} else if checkPointID != nil && !forceNewRun {
		cp, err := getCheckPointFromStore(ctx, *checkPointID, r.checkPointer)
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("load checkpoint from store fail: %w", err))
		}
		if cp != nil {
			// load checkpoint from store
			initialized = true

			err = r.checkPointer.restoreCheckPoint(cp, isStream)
			if err != nil {
				return nil, newGraphRunError(fmt.Errorf("restore checkpoint fail: %w", err))
			}

			err = cm.loadChannels(cp.Channels)
			if err != nil {
				return nil, newGraphRunError(err)
			}
			ctx = setStateModifier(ctx, stateModifier)
			ctx = setCheckPointToCtx(ctx, cp)
			if stateModifier != nil && cp.State != nil {
				err = stateModifier(ctx, *NewNodePath(), cp.State)
				if err != nil {
					return nil, newGraphRunError(fmt.Errorf("state modifier fail: %w", err))
				}
			}
			if cp.State != nil {
				ctx = context.WithValue(ctx, stateKey{}, &internalState{state: cp.State})
			}

			ctx, input = onGraphStart(ctx, input, isStream)
			haveOnStart = true
			// resume graph
			nextTasks, err = r.restoreTasks(ctx, cp.Inputs, cp.SkipPreHandler, cp.ToolsNodeExecutedTools, cp.RerunNodes, isStream, optMap)
			if err != nil {
				return nil, newGraphRunError(fmt.Errorf("restore tasks fail: %w", err))
			}
		}
	}
	if !initialized {
		// have not inited from checkpoint
		if r.runCtx != nil {
			ctx = r.runCtx(ctx)
		}

		ctx, input = onGraphStart(ctx, input, isStream)
		haveOnStart = true

		var isEnd bool
		nextTasks, result, isEnd, err = r.calculateNextTasks(ctx, []*task{{
			nodeKey: START,
			call:    r.inputChannels,
			output:  input,
		}}, isStream, cm, optMap)
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("calculate next tasks fail: %w", err))
		}
		if isEnd {
			return result, nil
		}
		if len(nextTasks) == 0 {
			return nil, newGraphRunError(fmt.Errorf("no tasks to execute after graph start"))
		}

		if keys := getHitKey(nextTasks, r.interruptBeforeNodes); len(keys) > 0 {
			tempInfo := newInterruptTempInfo()
			tempInfo.interruptBeforeNodes = append(tempInfo.interruptBeforeNodes, keys...)
			return nil, r.handleInterrupt(ctx,
				tempInfo,
				nextTasks,
				cm.channels,
				isStream,
				isSubGraph,
				writeToCheckPointID,
			)
		}
	}

	var lastCompletedTask []*task
	/*
	 * 主执行循环：图执行的核心控制逻辑
	 * 循环执行三个步骤：提交任务 -> 等待完成 -> 计算下一个任务
	 * 直到所有节点执行完成或遇到错误/中断
	 */
	for step := 0; ; step++ {
		/* 检查上下文取消状态，支持外部中断 */
		select {
		case <-ctx.Done():
			_ = tm.waitAll()
			return nil, newGraphRunError(fmt.Errorf("context has been canceled: %w", ctx.Err()))
		default:
		}
		/* 检查最大步数限制（DAG模式无此限制） */
		if !r.dag && step >= maxSteps {
			return nil, newGraphRunError(ErrExceedMaxSteps)
		}

		/*
		 * 三阶段执行模式：
		 * 1. 提交下一批待执行任务
		 * 2. 等待当前批次任务完成
		 * 3. 根据完成结果计算下一批任务
		 */

		/* 第一阶段：提交任务到任务管理器进行并发执行 */
		err = tm.submit(nextTasks)
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("failed to submit tasks: %w", err))
		}
		/* 第二阶段：等待当前批次的所有任务执行完成 */
		var completedTasks []*task
		completedTasks = tm.wait()

		/* 创建中断信息临时存储，用于跟踪各种中断状态 */
		tempInfo := newInterruptTempInfo()

		/*
		 * 解析已完成任务的中断状态
		 * 检测子图中断、重运行节点和执行后中断等情况
		 */
		err = r.resolveInterruptCompletedTasks(tempInfo, completedTasks)
		if err != nil {
			return nil, err // err has been wrapped
		}

		/*
		 * 处理子图中断和重运行节点的复合中断场景
		 * 需要等待所有任务完成，保存状态并返回中断信息
		 */
		if len(tempInfo.subGraphInterrupts)+len(tempInfo.interruptRerunNodes) > 0 {
			cpt := tm.waitAll()
			err = r.resolveInterruptCompletedTasks(tempInfo, cpt)
			if err != nil {
				return nil, err // err has been wrapped
			}

			/*
			 * 子图中断处理流程：
			 * 1. 保存其他已完成任务到通道
			 * 2. 将中断的子图保存为下次任务（跳过预处理器）
			 * 3. 报告当前图的中断信息
			 */
			return nil, r.handleInterruptWithSubGraphAndRerunNodes(
				ctx,
				tempInfo,
				append(completedTasks, cpt...),
				writeToCheckPointID,
				isSubGraph,
				cm,
				isStream,
			)
		}

		/* 验证是否有任务完成，防止死循环 */
		if len(completedTasks) == 0 {
			return nil, newGraphRunError(fmt.Errorf("no tasks to execute, last completed nodes: %v", printTask(lastCompletedTask)))
		}
		lastCompletedTask = completedTasks

		/*
		 * 第三阶段：根据已完成任务计算下一批待执行任务
		 * 这是图执行的核心调度逻辑，决定执行路径和数据流向
		 */
		var isEnd bool
		nextTasks, result, isEnd, err = r.calculateNextTasks(ctx, completedTasks, isStream, cm, optMap)
		if err != nil {
			return nil, newGraphRunError(fmt.Errorf("failed to calculate next tasks: %w", err))
		}
		/* 检查是否到达END节点，若是则返回最终结果 */
		if isEnd {
			return result, nil
		}

		tempInfo.interruptBeforeNodes = getHitKey(nextTasks, r.interruptBeforeNodes)

		if len(tempInfo.interruptBeforeNodes) > 0 || len(tempInfo.interruptAfterNodes) > 0 {
			newCompletedTasks := tm.waitAll()

			err = r.resolveInterruptCompletedTasks(tempInfo, newCompletedTasks)
			if err != nil {
				return nil, err // err has been wrapped
			}

			if len(tempInfo.subGraphInterrupts)+len(tempInfo.interruptRerunNodes) > 0 {
				return nil, r.handleInterruptWithSubGraphAndRerunNodes(
					ctx,
					tempInfo,
					append(completedTasks, newCompletedTasks...),
					writeToCheckPointID,
					isSubGraph,
					cm,
					isStream,
				)
			}

			var newNextTasks []*task
			newNextTasks, result, isEnd, err = r.calculateNextTasks(ctx, newCompletedTasks, isStream, cm, optMap)
			if err != nil {
				return nil, newGraphRunError(fmt.Errorf("failed to calculate next tasks: %w", err))
			}

			if isEnd {
				return result, nil
			}

			tempInfo.interruptBeforeNodes = append(tempInfo.interruptBeforeNodes, getHitKey(newNextTasks, r.interruptBeforeNodes)...)

			// simple interrupt
			return nil, r.handleInterrupt(ctx, tempInfo, append(nextTasks, newNextTasks...), cm.channels, isStream, isSubGraph, writeToCheckPointID)
		}
	}
}

func newInterruptTempInfo() *interruptTempInfo {
	return &interruptTempInfo{
		subGraphInterrupts:     map[string]*subGraphInterruptError{},
		interruptRerunExtra:    map[string]any{},
		interruptExecutedTools: make(map[string]map[string]string),
	}
}

type interruptTempInfo struct {
	subGraphInterrupts     map[string]*subGraphInterruptError
	interruptRerunNodes    []string
	interruptBeforeNodes   []string
	interruptAfterNodes    []string
	interruptRerunExtra    map[string]any
	interruptExecutedTools map[string]map[string]string
}

func (r *runner) resolveInterruptCompletedTasks(tempInfo *interruptTempInfo, completedTasks []*task) (err error) {
	for i := 0; i < len(completedTasks); i++ {
		if completedTasks[i].err != nil {
			if info := isSubGraphInterrupt(completedTasks[i].err); info != nil {
				tempInfo.subGraphInterrupts[completedTasks[i].nodeKey] = info
				continue
			}
			extra, ok := IsInterruptRerunError(completedTasks[i].err)
			if ok {
				tempInfo.interruptRerunNodes = append(tempInfo.interruptRerunNodes, completedTasks[i].nodeKey)
				if extra != nil {
					tempInfo.interruptRerunExtra[completedTasks[i].nodeKey] = extra

					// save tool node info
					if completedTasks[i].call.action.meta.component == ComponentOfToolsNode {
						if e, ok := extra.(*ToolsInterruptAndRerunExtra); ok {
							tempInfo.interruptExecutedTools[completedTasks[i].nodeKey] = e.ExecutedTools
						}
					}
				}
				continue
			}
			return wrapGraphNodeError(completedTasks[i].nodeKey, completedTasks[i].err)
		}
		for _, key := range r.interruptAfterNodes {
			if key == completedTasks[i].nodeKey {
				tempInfo.interruptAfterNodes = append(tempInfo.interruptAfterNodes, key)
				break
			}
		}
	}
	return nil
}

func getHitKey(tasks []*task, keys []string) []string {
	var ret []string
	for _, t := range tasks {
		for _, key := range keys {
			if key == t.nodeKey {
				ret = append(ret, t.nodeKey)
			}
		}
	}
	return ret
}

func (r *runner) handleInterrupt(
	ctx context.Context,
	tempInfo *interruptTempInfo,
	nextTasks []*task,
	channels map[string]channel,
	isStream bool,
	isSubGraph bool,
	checkPointID *string,
) error {
	cp := &checkpoint{
		Channels:       channels,
		Inputs:         make(map[string]any),
		SkipPreHandler: map[string]bool{},
	}
	if r.runCtx != nil {
		// current graph has enable state
		if state, ok := ctx.Value(stateKey{}).(*internalState); ok {
			cp.State = state.state
		}
	}
	intInfo := &InterruptInfo{
		State:           cp.State,
		AfterNodes:      tempInfo.interruptAfterNodes,
		BeforeNodes:     tempInfo.interruptBeforeNodes,
		RerunNodes:      tempInfo.interruptRerunNodes,
		RerunNodesExtra: tempInfo.interruptRerunExtra,
		SubGraphs:       make(map[string]*InterruptInfo),
	}
	for _, t := range nextTasks {
		cp.Inputs[t.nodeKey] = t.input
	}
	err := r.checkPointer.convertCheckPoint(cp, isStream)
	if err != nil {
		return fmt.Errorf("failed to convert checkpoint: %w", err)
	}
	if isSubGraph {
		return &subGraphInterruptError{
			Info:       intInfo,
			CheckPoint: cp,
		}
	} else if checkPointID != nil {
		err := r.checkPointer.set(ctx, *checkPointID, cp)
		if err != nil {
			return fmt.Errorf("failed to set checkpoint: %w, checkPointID: %s", err, *checkPointID)
		}
	}
	return &interruptError{Info: intInfo}
}

func (r *runner) handleInterruptWithSubGraphAndRerunNodes(
	ctx context.Context,
	tempInfo *interruptTempInfo,
	completeTasks []*task,
	checkPointID *string,
	isSubGraph bool,
	cm *channelManager,
	isStream bool,
) error {
	var rerunTasks, subgraphTasks, otherTasks []*task
	skipPreHandler := map[string]bool{}
	for _, t := range completeTasks {
		if _, ok := tempInfo.subGraphInterrupts[t.nodeKey]; ok {
			subgraphTasks = append(subgraphTasks, t)
			skipPreHandler[t.nodeKey] = true // subgraph won't run pre-handler again, but rerun nodes will
			continue
		}
		rerun := false
		for _, key := range tempInfo.interruptRerunNodes {
			if key == t.nodeKey {
				rerunTasks = append(rerunTasks, t)
				rerun = true
				break
			}
		}
		if !rerun {
			otherTasks = append(otherTasks, t)
		}
	}

	// forward completed tasks
	toValue, controls, err := r.resolveCompletedTasks(ctx, otherTasks, isStream, cm)
	if err != nil {
		return fmt.Errorf("failed to resolve completed tasks in interrupt: %w", err)
	}
	err = cm.updateValues(ctx, toValue)
	if err != nil {
		return fmt.Errorf("failed to update values in interrupt: %w", err)
	}
	err = cm.updateDependencies(ctx, controls)
	if err != nil {
		return fmt.Errorf("failed to update dependencies in interrupt: %w", err)
	}

	cp := &checkpoint{
		Channels:               cm.channels,
		Inputs:                 make(map[string]any),
		SkipPreHandler:         skipPreHandler,
		ToolsNodeExecutedTools: tempInfo.interruptExecutedTools,
		SubGraphs:              make(map[string]*checkpoint),
	}
	if r.runCtx != nil {
		// current graph has enable state
		if state, ok := ctx.Value(stateKey{}).(*internalState); ok {
			cp.State = state.state
		}
	}

	intInfo := &InterruptInfo{
		State:           cp.State,
		BeforeNodes:     tempInfo.interruptBeforeNodes,
		AfterNodes:      tempInfo.interruptAfterNodes,
		RerunNodes:      tempInfo.interruptRerunNodes,
		RerunNodesExtra: tempInfo.interruptRerunExtra,
		SubGraphs:       make(map[string]*InterruptInfo),
	}
	for _, t := range subgraphTasks {
		cp.RerunNodes = append(cp.RerunNodes, t.nodeKey)
		cp.SubGraphs[t.nodeKey] = tempInfo.subGraphInterrupts[t.nodeKey].CheckPoint
		intInfo.SubGraphs[t.nodeKey] = tempInfo.subGraphInterrupts[t.nodeKey].Info
	}
	for _, t := range rerunTasks {
		cp.RerunNodes = append(cp.RerunNodes, t.nodeKey)
	}
	err = r.checkPointer.convertCheckPoint(cp, isStream)
	if err != nil {
		return fmt.Errorf("failed to convert checkpoint: %w", err)
	}
	if isSubGraph {
		return &subGraphInterruptError{
			Info:       intInfo,
			CheckPoint: cp,
		}
	} else if checkPointID != nil {
		err = r.checkPointer.set(ctx, *checkPointID, cp)
		if err != nil {
			return fmt.Errorf("failed to set checkpoint: %w, checkPointID: %s", err, *checkPointID)
		}
	}
	return &interruptError{Info: intInfo}
}

/**
 * 计算下一批待执行任务的核心调度方法
 * 根据已完成任务的输出，更新通道状态并确定下一步执行路径
 *
 * @param ctx 执行上下文，包含状态和配置信息
 * @param completedTasks 已完成的任务列表，包含输出结果
 * @param isStream 是否为流式处理模式
 * @param cm 通道管理器，负责节点间数据流转
 * @param optMap 节点选项映射，为特定节点提供执行参数
 * @return nextTasks 下一批待执行的任务列表
 * @return result 如果到达END节点，返回最终结果
 * @return isEnd 是否已到达图的结束节点
 * @return err 计算过程中的错误信息
 */
func (r *runner) calculateNextTasks(ctx context.Context, completedTasks []*task, isStream bool, cm *channelManager, optMap map[string][]any) ([]*task, any, bool, error) {
	/*
	 * 解析已完成任务的输出结果
	 * 将任务输出转换为通道写入值和控制依赖关系
	 */
	writeChannelValues, controls, err := r.resolveCompletedTasks(ctx, completedTasks, isStream, cm)
	if err != nil {
		return nil, nil, false, err
	}
	/*
	 * 更新通道状态并获取可执行的节点映射
	 * 通道管理器根据依赖关系决定哪些节点已准备好执行
	 */
	nodeMap, err := cm.updateAndGet(ctx, writeChannelValues, controls)
	if err != nil {
		return nil, nil, false, fmt.Errorf("failed to update and get channels: %w", err)
	}
	var nextTasks []*task
	if len(nodeMap) > 0 {
		/* 检查是否到达END节点，若是则返回最终执行结果 */
		if v, ok := nodeMap[END]; ok {
			return nil, v, true, nil
		}

		/*
		 * 创建下一批执行任务
		 * 为每个准备好的节点创建对应的任务实例
		 */
		nextTasks, err = r.createTasks(ctx, nodeMap, optMap)
		if err != nil {
			return nil, nil, false, fmt.Errorf("failed to create tasks: %w", err)
		}
	}
	return nextTasks, nil, false, nil
}

func (r *runner) createTasks(ctx context.Context, nodeMap map[string]any, optMap map[string][]any) ([]*task, error) {
	var nextTasks []*task
	for nodeKey, nodeInput := range nodeMap {
		call, ok := r.chanSubscribeTo[nodeKey]
		if !ok {
			return nil, fmt.Errorf("node[%s] has not been registered", nodeKey)
		}

		if call.action.nodeInfo != nil && call.action.nodeInfo.compileOption != nil {
			ctx = forwardCheckPoint(ctx, nodeKey)
		}

		nextTasks = append(nextTasks, &task{
			ctx:     setNodeKey(ctx, nodeKey),
			nodeKey: nodeKey,
			call:    call,
			input:   nodeInput,
			option:  optMap[nodeKey],
		})
	}
	return nextTasks, nil
}

func getCheckPointInfo(opts ...Option) (checkPointID *string, writeToCheckPointID *string, stateModifier StateModifier, forceNewRun bool) {
	for _, opt := range opts {
		if opt.checkPointID != nil {
			checkPointID = opt.checkPointID
		}
		if opt.writeToCheckPointID != nil {
			writeToCheckPointID = opt.writeToCheckPointID
		}
		if opt.stateModifier != nil {
			stateModifier = opt.stateModifier
		}
		forceNewRun = opt.forceNewRun
	}
	if writeToCheckPointID == nil {
		writeToCheckPointID = checkPointID
	}
	return
}

func (r *runner) restoreTasks(
	ctx context.Context,
	inputs map[string]any,
	skipPreHandler map[string]bool,
	toolNodeExecutedTools map[string]map[string]string,
	rerunNodes []string,
	isStream bool,
	optMap map[string][]any) ([]*task, error) {
	ret := make([]*task, 0, len(inputs))
	for _, key := range rerunNodes {
		call, ok := r.chanSubscribeTo[key]
		if !ok {
			return nil, fmt.Errorf("channel[%s] from checkpoint is not registered", key)
		}
		if isStream {
			inputs[key] = call.action.inputEmptyStream()
		} else {
			inputs[key] = call.action.inputZeroValue()
		}
	}
	for key, input := range inputs {
		call, ok := r.chanSubscribeTo[key]
		if !ok {
			return nil, fmt.Errorf("channel[%s] from checkpoint is not registered", key)
		}

		if call.action.nodeInfo != nil && call.action.nodeInfo.compileOption != nil {
			// sub graph
			ctx = forwardCheckPoint(ctx, key)
		}

		newTask := &task{
			ctx:            setNodeKey(ctx, key),
			nodeKey:        key,
			call:           call,
			input:          input,
			option:         nil,
			skipPreHandler: skipPreHandler[key],
		}
		if opt, ok := optMap[key]; ok {
			newTask.option = opt
		}
		if executedTools, ok := toolNodeExecutedTools[key]; ok {
			newTask.option = append(newTask.option, withExecutedTools(executedTools))
		}

		ret = append(ret, newTask)
	}
	return ret, nil
}

/**
 * 解析已完成任务并生成通道写入值和依赖关系
 * 处理任务输出的分发，包括分支计算和数据复制逻辑
 *
 * @param ctx 执行上下文
 * @param completedTasks 已完成的任务列表
 * @param isStream 是否为流式处理模式
 * @param cm 通道管理器实例
 * @return writeChannelValues 需要写入各通道的值映射
 * @return newDependencies 新产生的依赖关系映射
 * @return err 处理过程中的错误信息
 */
func (r *runner) resolveCompletedTasks(ctx context.Context, completedTasks []*task, isStream bool, cm *channelManager) (map[string]map[string]any, map[string][]string, error) {
	writeChannelValues := make(map[string]map[string]any)
	newDependencies := make(map[string][]string)

	/* 遍历所有已完成的任务，处理其输出和依赖关系 */
	for _, t := range completedTasks {
		/* 处理控制依赖：记录当前任务对其控制目标的依赖关系 */
		for _, key := range t.call.controls {
			newDependencies[key] = append(newDependencies[key], t.nodeKey)
		}

		/*
		 * 准备任务输出的多份副本
		 * 为直接写入目标和分支目标分别创建输出副本
		 */
		vs := copyItem(t.output, len(t.call.writeTo)+len(t.call.writeToBranches)*2)

		/*
		 * 计算分支逻辑确定的下一步节点
		 * 根据分支条件动态决定数据流向
		 */
		nextNodeKeys, err := r.calculateBranch(ctx, t.nodeKey, t.call,
			vs[len(t.call.writeTo)+len(t.call.writeToBranches):], isStream, cm)
		if err != nil {
			return nil, nil, fmt.Errorf("calculate next step fail, node: %s, error: %w", t.nodeKey, err)
		}

		/* 为分支计算出的节点建立依赖关系 */
		for _, key := range nextNodeKeys {
			newDependencies[key] = append(newDependencies[key], t.nodeKey)
		}
		/* 合并直接写入目标和分支计算出的目标节点 */
		nextNodeKeys = append(nextNodeKeys, t.call.writeTo...)

		/*
		 * 处理多后继节点的数据分发
		 * 当分支产生多个后继节点时，需要相应地复制输入数据
		 */
		if len(nextNodeKeys) > 0 {
			toCopyNum := len(nextNodeKeys) - len(t.call.writeTo) - len(t.call.writeToBranches)
			nVs := copyItem(vs[len(t.call.writeTo)+len(t.call.writeToBranches)-1], toCopyNum+1)
			vs = append(vs[:len(t.call.writeTo)+len(t.call.writeToBranches)-1], nVs...)

			/* 为每个后继节点分配对应的输出数据副本 */
			for i, next := range nextNodeKeys {
				if _, ok := writeChannelValues[next]; !ok {
					writeChannelValues[next] = make(map[string]any)
				}
				writeChannelValues[next][t.nodeKey] = vs[i]
			}
		}
	}
	return writeChannelValues, newDependencies, nil
}

func (r *runner) calculateBranch(ctx context.Context, curNodeKey string, startChan *chanCall, input []any, isStream bool, cm *channelManager) ([]string, error) {
	if len(input) < len(startChan.writeToBranches) {
		// unreachable
		return nil, errors.New("calculate next input length is shorter than branches")
	}

	ret := make([]string, 0, len(startChan.writeToBranches))

	skippedNodes := make(map[string]struct{})
	for i, branch := range startChan.writeToBranches {
		// check branch input type if needed
		var err error
		input[i], err = r.preBranchHandlerManager.handle(curNodeKey, i, input[i], isStream)
		if err != nil {
			return nil, fmt.Errorf("branch[%s]-[%d] pre handler fail: %w", curNodeKey, branch.idx, err)
		}

		// process branch output
		var ws []string
		if isStream {
			ws, err = branch.collect(ctx, input[i].(streamReader))
			if err != nil {
				return nil, fmt.Errorf("branch collect run error: %w", err)
			}
		} else {
			ws, err = branch.invoke(ctx, input[i])
			if err != nil {
				return nil, fmt.Errorf("branch invoke run error: %w", err)
			}
		}

		for node := range branch.endNodes {
			skipped := true
			for _, w := range ws {
				if node == w {
					skipped = false
					break
				}
			}
			if skipped {
				skippedNodes[node] = struct{}{}
			}
		}

		ret = append(ret, ws...)
	}

	// When a node has multiple branches,
	// there may be a situation where a succeeding node is selected by some branches and discarded by the other branches,
	// in which case the succeeding node should not be skipped.
	var skippedNodeList []string
	for _, selected := range ret {
		if _, ok := skippedNodes[selected]; ok {
			delete(skippedNodes, selected)
		}
	}
	for skipped := range skippedNodes {
		skippedNodeList = append(skippedNodeList, skipped)
	}

	err := cm.reportBranch(curNodeKey, skippedNodeList)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (r *runner) initTaskManager(runWrapper runnableCallWrapper, opts ...Option) *taskManager {
	return &taskManager{
		runWrapper: runWrapper,
		opts:       opts,
		needAll:    !r.eager,
		done:       internal.NewUnboundedChan[*task](),
	}
}

func (r *runner) initChannelManager(isStream bool) *channelManager {
	builder := r.chanBuilder
	if builder == nil {
		builder = pregelChannelBuilder
	}

	chs := make(map[string]channel)
	for ch := range r.chanSubscribeTo {
		chs[ch] = builder(r.controlPredecessors[ch], r.dataPredecessors[ch], r.chanSubscribeTo[ch].action.inputZeroValue, r.chanSubscribeTo[ch].action.inputEmptyStream)
	}

	chs[END] = builder(r.controlPredecessors[END], r.dataPredecessors[END], r.outputZeroValue, r.outputEmptyStream)

	dataPredecessors := make(map[string]map[string]struct{})
	for k, vs := range r.dataPredecessors {
		dataPredecessors[k] = make(map[string]struct{})
		for _, v := range vs {
			dataPredecessors[k][v] = struct{}{}
		}
	}
	controlPredecessors := make(map[string]map[string]struct{})
	for k, vs := range r.controlPredecessors {
		controlPredecessors[k] = make(map[string]struct{})
		for _, v := range vs {
			controlPredecessors[k][v] = struct{}{}
		}
	}

	for k, v := range chs {
		if cfg, ok := r.mergeConfigs[k]; ok {
			v.setMergeConfig(cfg)
		}
	}

	return &channelManager{
		isStream:            isStream,
		channels:            chs,
		successors:          r.successors,
		dataPredecessors:    dataPredecessors,
		controlPredecessors: controlPredecessors,

		edgeHandlerManager:    r.edgeHandlerManager,
		preNodeHandlerManager: r.preNodeHandlerManager,
	}
}

func (r *runner) toComposableRunnable() *composableRunnable {
	cr := &composableRunnable{
		i: func(ctx context.Context, input any, opts ...any) (output any, err error) {
			tos, err := convertOption[Option](opts...)
			if err != nil {
				return nil, err
			}
			return r.invoke(ctx, input, tos...)
		},
		t: func(ctx context.Context, input streamReader, opts ...any) (output streamReader, err error) {
			tos, err := convertOption[Option](opts...)
			if err != nil {
				return nil, err
			}
			return r.transform(ctx, input, tos...)
		},

		inputType:     r.inputType,
		outputType:    r.outputType,
		genericHelper: r.genericHelper,
		optionType:    nil, // if option type is nil, graph will transmit all options.
	}

	return cr
}

func copyItem(item any, n int) []any {
	if n < 2 {
		return []any{item}
	}

	ret := make([]any, n)
	if s, ok := item.(streamReader); ok {
		ss := s.copy(n)
		for i := range ret {
			ret[i] = ss[i]
		}

		return ret
	}

	for i := range ret {
		ret[i] = item
	}

	return ret
}

func printTask(ts []*task) string {
	if len(ts) == 0 {
		return "[]"
	}
	sb := strings.Builder{}
	sb.WriteString("[")
	for i := 0; i < len(ts)-1; i++ {
		sb.WriteString(ts[i].nodeKey)
		sb.WriteString(", ")
	}
	sb.WriteString(ts[len(ts)-1].nodeKey)
	sb.WriteString("]")
	return sb.String()
}
