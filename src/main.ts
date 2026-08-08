import '@gershy/clearing';
import { Pollen, type PollenInp } from '@gershy/pollen';
import type { HttpMethod } from '@gershy/util-http';
import codecParse from '@gershy/util-codec-parse';

export type SoktEvt = { t: 'accept', val: Json } | { t: 'reject', err: any };
export type SoktPollenDef = {
  addr: string,
  port?: number,
  http?: {
    path?: string[],
    method?: HttpMethod
  },
  sokt: WebSocket & { buff: SoktEvt[], evtPrm: PromiseLater<'active' | 'finish'> }
};
export class PollenSokt extends Pollen<SoktPollenDef> {
  
  // Can call sokt scripts
  
  constructor(inp: PollenInp<'domain' | 'awsApiGateway' | 'awsCloudfrontDistribution'>) { super(inp); }
  
  protected async sanitizeDef(def: unknown) {
    
    const { addr, port = 443, http: { path = [] } = {} } = codecParse({ type: 'rec', loose: true, props: {
      addr: { type: 'str' },
      port: { req: false, type: 'num' },
      http: { req: false, type: 'rec', loose: true, props: {
        path:   { req: false, type: 'arr', item: { type: 'str' } },
        method: { req: false, type: 'enum', opts: [ 'head', 'get', 'post', 'put', 'patch', 'delete' ] }
      }}
    }} as const, def);
    
    const url = `${port === 443 ? 'wss' : 'ws'}://${addr}${path.length ? '/' : ''}${path.join('/')}`;
    const sokt = Object.assign(new WebSocket(url), {
      info: { url },
      buff: [] as SoktEvt[],
      evtPrm: Promise[cl.later]<'active' | 'finish'>()
    });
    
    const firstErrPrm = Promise[cl.later]<Error>();
    firstErrPrm.catch(() => { /* must never throw */ });
    
    const update = (term: 'active' | 'finish') => {
      const prevPrm = sokt.evtPrm;
      sokt.evtPrm = Promise[cl.later]();
      prevPrm.resolve(term);
    };
    
    sokt.addEventListener('message', evt => {
      
      try              { sokt.buff.push({ t: 'accept', val: JSON.parse(evt.data) }); }
      catch (err: any) { sokt.buff.push({ t: 'reject', err: err[cl.mod]({ evt }) }); }
      
      update('active');
      
    });
    sokt.addEventListener('close', () => {
      
      // Note that once `sokt.evtPrm` has resolved to "finish" it is immutable and finalized (no other sokt events will occur)
      update('finish');
      
    });
    sokt.addEventListener('error', (cause: any) => {
      
      cause.catch?.();
      
      const err = Error('sokt event reject')
        [cl.mod]({ cause, sokt: sokt[cl.slice]([ 'binaryType', 'info' ]) })
        [cl.suppress]();
      
      sokt.buff.push({ t: 'reject', err });
      update('active');
      
      firstErrPrm.reject(err);
      
    });
    
    // Wait for the sokt to open - rejects if an error occurs before the sokt is open
    await new Promise((rsv, rjc) => {
      sokt.addEventListener('open', rsv);
      firstErrPrm.catch(err => rjc(err));
    });
    
    return {
      addr,
      port,
      http: { path },
      sokt: sokt as SoktPollenDef['sokt'] // Needed for declaration typing
    };
    
  }
  
  public async finish() {
    
    const { defPrm } = this;
    if (!defPrm) return;
    
    this.defPrm = null;
      
    const { sokt } = await defPrm;
    const closedPrm = (async () => { while (true) if (await sokt.evtPrm === 'finish') break; })(); // Resolve after seeing "finish" event
    sokt.close();
    await closedPrm;
    
    
  }
  public async fly(args: Json) {
    
    // Consider: rename fly/notice -> pistil/stamen?
    
    args = JSON.stringify(args);
    const { sokt } = await this.getDef();
    sokt.send(args)
  }
  public async * notice() {
    
    const { sokt } = await this.getDef();
    
    while (true) {
      
      // Drain all events
      while (sokt.buff.length) {
        const v = sokt.buff.shift()!;
        if (v.t === 'reject') throw Error('sokt reject')[cl.mod]({ cause: v.err, unprocessedEvents: sokt.buff });
        yield v.val;
      }
      
      const state = await sokt.evtPrm; // `sokt.evtPrm` both waits for activity and reports whether the sokt is still alive
      if (state === 'finish') break; // Note that `this.launch().evtPrm` *must* eventually resolve to "finish"!
      
    }
    
  }
  
  protected getJsfnHoist() { return `${import.meta.filename}::{${this.constructor.name}}` as const; }
  protected getJsfnInp() { return {}; }
  
};