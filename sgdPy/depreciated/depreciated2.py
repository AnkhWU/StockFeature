if index_wave.state == "WAVELESS":
    return

INDEX = index_wave.ticker
len_currentwave = copy.copy(len(self.stockFeature.dequeCurrentWave[INDEX]))

wave_value = index_wave.lastprice / index_wave.waveStartPrice - 1
indexStartTime = index_wave.startTime
indexCurrentTime = index_wave.timestamp

# INDEX非WAVELESS的状态下，更新所有成分股在indexCurrentTime的最后价格
for TICKER in self.index_components[INDEX]:
    component_ticks = self.stockFeature.bufferDict[TICKER]
    if not len(component_ticks) == 0:
        for component_tick in reversed(component_ticks):
            if component_tick.timestamp <= indexCurrentTime:
                self.tempIndexWave[INDEX][TICKER]['last_price'] = component_tick.new_price

# len_currentwave 为 1，确定为新趋势产生
if len_currentwave == 1:
    for TICKER in self.index_components[INDEX]:
        component_waves = self.stockFeature.dequeWaveRecord[TICKER]
        if len(component_waves) > 0:
            for component_wave in reversed(component_waves):
                if component_wave.timestamp <= indexStartTime:
                    if component_wave.state == "WAVELESS":
                        self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.lastprice
                    else:
                        self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.waveStartPrice
                    break
                try:
                    self.tempIndexWave[INDEX][TICKER]["target_price"] = component_waves[0].lastprice
                except:
                    pass
        else:
            try:
                self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.bufferDict[TICKER][0].new_price
            except:
                pass

self.tempIndexTime[INDEX].append(indexStartTime)

if wave_value == 0:
    return

if len_currentwave > 1:
    if indexStartTime == self.tempIndexTime[INDEX][-2]:
        for TICKER in self.index_components[INDEX]:
            if not self.tempIndexWave[INDEX][TICKER]["target_price"]:
                try:
                    self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.bufferDict[TICKER][
                        0].new_price
                except:
                    pass
    else:
        for TICKER in self.index_components[INDEX]:
            component_waves = self.stockFeature.dequeWaveRecord[TICKER]
            if len(component_waves) > 0:
                for component_wave in reversed(component_waves):
                    if component_wave.timestamp <= indexStartTime:
                        if component_wave.state == "WAVELESS":
                            self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.lastprice
                        else:
                            self.tempIndexWave[INDEX][TICKER]['target_price'] = component_wave.waveStartPrice
                        break
                    try:
                        self.tempIndexWave[INDEX][TICKER]["target_price"] = component_waves[0].lastprice
                    except:
                        pass
            else:
                try:
                    self.tempIndexWave[INDEX][TICKER]["target_price"] = self.stockFeature.bufferDict[TICKER][
                        0].new_price
                except:
                    pass