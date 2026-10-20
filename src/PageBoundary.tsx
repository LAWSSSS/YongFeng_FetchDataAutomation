import { Component, type ReactNode } from 'react';

export default class PageBoundary extends Component<{children: ReactNode}, {failed: boolean}> {
  state = {failed:false};
  static getDerivedStateFromError() {return {failed:true};}
  render() {
    if (this.state.failed) return <section className="panel page-recovery" role="alert">
      <h2>页面未能打开</h2>
      <p>页面文件未完整加载或显示出现异常。任务与更正进度已保存在本机，刷新后可继续查看。</p>
      <button onClick={()=>window.location.reload()}>重新加载页面</button>
    </section>;
    return this.props.children;
  }
}
