/* 
 * SparkBit's Bitcoinj
 *
 * Copyright 2014 Coin Sciences Ltd.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.coinspark.wallet;

import com.google.common.eventbus.*;
import java.util.concurrent.*;

/**
 * Coinspark Event Bus
 */
public enum CSEventBus {

    INSTANCE;

	    
//    private final EventBus eventBus;
    private final AsyncEventBus asyncEventBus;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    CSEventBus() {
//	eventBus = new EventBus();
	asyncEventBus = new AsyncEventBus(executorService);
    }

//    public EventBus getEventBus() {
//	return eventBus;
//    }
//
//    public EventBus getAsyncEventBus() {
//	return asyncEventBus;
//    }
//    
//    public void registerSubscriber(Object o) {
//	eventBus.register(o);
//    }
    
    public void registerAsyncSubscriber(Object o) {
	asyncEventBus.register(o);
    }
    
    public void unsubscribe(Object o) {
	asyncEventBus.unregister(o);
    }

//    public void post(Object o) {
//	eventBus.post(o);
//    }
    
    public void postAsync(Object o) {
	asyncEventBus.post(o);
    }
    
//    public void postEvent(int type) {
//	eventBus.post(new CSEvent(type));
//    }
//
//    public void postEvent(int type, Object info) {
//	eventBus.post(new CSEvent(type, info));
//    }

    public void postAsyncEvent(CSEventType type) {
	asyncEventBus.post(new CSEvent(type));
    }

    public void postAsyncEvent(CSEventType type, Object info) {
	asyncEventBus.post(new CSEvent(type, info));
    }


    
    

//    class ThreadPerTaskExecutor implements Executor {
//	@Override
//	public void execute(Runnable r) {
//	    new Thread(r).start();
//	}
//    }

}
