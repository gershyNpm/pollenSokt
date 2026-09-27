import '@gershy/clearing';
import { Pollen, type PollenInp } from '@gershy/pollen';
import codecParse from '@gershy/util-codec-parse';
import type { HttpMethod } from '@gershy/util-http';

// type Soktttt = WebSocket;
export type Sokt = { // Custom bare-minimum socket client type, to avoid dealing with the `global.WebSocket` / `require('undici').WebSocket` duality
  // binaryType: 'blob' | 'arraybuffer',
  close: (code?: number, reason?: string) => void,
  send: (msg: string) => void,
  addEventListener: {
    (type: 'message', cb: (msg: { data: string                  }) => void): void,
    (type: 'close',   cb: (                                      ) => void): void,
    (type: 'error',   cb: (err: { message: string, error: Error }) => void): void,
    (type: 'open',    cb: (                                      ) => void): void,
  }
};
export type SoktCls = { new (url: string): Sokt };

export type SoktEvt = { t: 'accept', val: Json } | { t: 'reject', err: any };
export type SoktPollenDef = {
  addr: string,
  port?: number,
  http?: {
    path?: string[],
    method?: HttpMethod
  },
  sokt: Sokt & { buff: SoktEvt[], evtPrm: PromiseLater<'active' | 'finish'> }
};
export class PollenSokt extends Pollen<SoktPollenDef> {
  
  // TODO: avoid generator?? Sequential messages / errors / etc can prolly be a LL of promises????
  
  // Can call sokt scripts
  protected SoktCls: { new (url: string): Sokt }; // TODO: can't be jsfn-serialized
  constructor(inp: PollenInp<'domain' | 'awsApiGateway' | 'awsCloudfrontDistribution'> & { SoktCls?: SoktCls }) {
    super(inp);
    this.SoktCls = inp.SoktCls ?? global.WebSocket;
  }
  
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
    
    const { SoktCls } = this;
    const sokt: SoktPollenDef['sokt'] = Object.assign(new SoktCls(url), {
      
      info: { url },
      buff: [] as SoktEvt[],
      evtPrm: Promise[cl.later]<'active' | 'finish'>()
      
    });
    
    const firstErrPrm = Promise[cl.later]<Error>();
    firstErrPrm.catch(() => { /* must never throw */ });
    
    let finished = false;
    const update = (term: 'active' | 'finish', d?: any) => {
      
      if (finished) return console.log('UPDATE AFTER FINISHED??', term, d); // TODO: remove `console.log`
      
      const prevPrm = sokt.evtPrm;
      
      if (term === 'finish') finished = true;
      else                   sokt.evtPrm = Promise[cl.later]();
      
      prevPrm.resolve(term);
      
    };
    
    sokt.addEventListener('message', evt => {
      
      try              { sokt.buff.push({ t: 'accept', val: JSON.parse(evt.data) }); }
      catch (err: any) { sokt.buff.push({ t: 'reject', err: err[cl.mod]({ evt }) }); }
      
      update('active', evt);
      
    });
    sokt.addEventListener('close', () => {
      
      // Note that once `sokt.evtPrm` has resolved to "finish" it is immutable and finalized (no other sokt events will occur)
      update('finish');
      
    });
    sokt.addEventListener('error', ({ message, error: err }) => {
      
      sokt.buff.push({ t: 'reject', err });
      update('active', err);
      
      firstErrPrm.reject(err);
      
    });
    
    // Wait for the sokt to open - rejects if an error occurs before the sokt is open
    await new Promise<void>((rsv, rjc) => {
      sokt.addEventListener('open', rsv);
      firstErrPrm.catch(err => rjc(err));
    });
    
    return {
      addr,
      port,
      http: { path },
      sokt: sokt as SoktPollenDef['sokt'] // This type is needed for declaration typing... ouch.
    };
    
  }
  
  public async cancel() {
    
    const { defPrm } = this;
    if (!defPrm) return;
    this.defPrm = null;
    
    const { sokt } = await defPrm;
    const closedPrm = (async () => { while (true) if (await sokt.evtPrm === 'finish') break; })(); // Resolve after seeing "finish" event
    sokt.close();
    await closedPrm;
    
  }
  public async tell(args: Json) { (await this.getDef()).sokt.send(JSON.stringify(args)); }
  public async fly(args: Json) { return this.tell(args); }
  public async * hear() {
    
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