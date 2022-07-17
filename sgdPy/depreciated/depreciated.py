        INDEX =  index_wave.ticker
        if len(self.stockFeature.bufferDict[INDEX]) <= 5:
            for TICKER in self.index_components[INDEX]:
                if len(self.stockFeature.dequeCurrentWave[TICKER]) != 0:
                    self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.dequeCurrentWave[TICKER][
                        -1].lastprice
            return
        try:
            last_state = self.stockFeature.dequeCurrentWave[INDEX][-2].state
        except:
            last_state = self.stockFeature.dequeLast1Wave[INDEX][-1].state

        if index_wave.state == "WAVELESS":
            wave_value = index_wave.lastprice / self.stockFeature.dequeCurrentWave[INDEX][0].lastprice - 1
            indexStartTime = self.stockFeature.dequeCurrentWave[INDEX][0].timestamp
        else:
            wave_value = index_wave.lastprice / index_wave.waveStartPrice - 1
            indexStartTime = index_wave.startTime

        if wave_value == 0:
            return

        if index_wave.state != last_state or len(self.tempIndexWave[INDEX]) == 0:
            for TICKER in self.index_components[INDEX]:
                component_waves = self.stockFeature.dequeWaveRecord[TICKER]
                for component_wave in reversed(component_waves):
                    if component_wave.timestamp <= index_wave.timestamp:
                        self.tempIndexWave[INDEX][TICKER]['last_price'] = component_wave.lastprice
                        break

                if len(component_waves) >= 1:
                    if component_waves[0].timestamp > indexStartTime:
                        self.tempIndexWave[INDEX][TICKER]["target_price"] = component_waves[0].lastprice
                    else:
                        for component_wave in reversed(component_waves):
                            if component_wave.timestamp <= indexStartTime:
                                if component_wave.state == "WAVELESS":
                                    self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.lastprice
                                else:
                                    self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.waveStartPrice
                                break
                else:
                    try:
                        self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.dequeCurrentWave[TICKER][
                            0].lastprice
                    except:
                        pass
        else:
            for TICKER in self.index_components[INDEX]:
                component_waves = self.stockFeature.dequeWaveRecord[TICKER]
                for component_wave in reversed(component_waves):
                    if component_wave.timestamp <= index_wave.timestamp:
                        self.tempIndexWave[INDEX][TICKER]['last_price'] = component_wave.lastprice
                        break


        res_df = pd.DataFrame.from_dict(self.tempIndexWave[INDEX], "index")

        if res_df.empty:
            await asyncio.sleep(0)
            return
        else:
            res_df = res_df.dropna(axis = 0)
            res_df['INDEX'] = INDEX
            res_df['index_id'] = INDEX
            res_df['INDEX_VALUE'] = wave_value
            res_df.index.name = "order_book_id"
            try:
                res_df = pd.merge(res_df, self.preclose_df, left_index = True, right_index=True)
                res_df['daily_return'] = res_df['last_price'] / res_df['preclose'] - 1
            except Exception as e:
                print(e)
            res_df.reset_index(inplace = True)
            res_df['ticker'] = res_df['order_book_id']
            res_df['ratio'] = (res_df['last_price'] / res_df['target_price'] - 1) / res_df['INDEX_VALUE']

            res_df.reset_index(inplace = True)
            index_time = datetime.utcfromtimestamp(index_wave.timestamp / 1e9)
            res_df.index = pd.date_range(index_time, index_time, len(res_df))
            LOGGER.info(f"Ratio Data is written at {datetime.now()}")

            await self.__DBClient.write(res_df, measurement = 'IndexState', tag_columns = ['order_book_id', 'INDEX'])
            import random
random.randrange
import math
math.sq