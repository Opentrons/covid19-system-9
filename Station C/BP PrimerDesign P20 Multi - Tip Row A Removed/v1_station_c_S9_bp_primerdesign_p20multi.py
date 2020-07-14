from opentrons import protocol_api
import json
import os
import math

# metadata
metadata = {
    'protocolName': 'Version 1 S9 Station C BP PrimerDesign P20 Multi',
    'author': 'Nick <protocols@opentrons.com>',
    'source': 'Custom Protocol Request',
    'apiLevel': '2.3'
}

NUM_SAMPLES = 96  # start with 8 samples, slowly increase to 48, then 94 (max is 94)
PREPARE_MASTERMIX = True
TIP_TRACK = True


def run(ctx: protocol_api.ProtocolContext):
    global MM_TYPE

    # check source (elution) labware type
    source_plate = ctx.load_labware(
        'opentrons_96_aluminumblock_nest_wellplate_100ul', '1',
        'chilled elution plate on block from Station B')
    tips20 = [
        ctx.load_labware('opentrons_96_filtertiprack_20ul', slot)
        for slot in ['3', '6', '7', '8', '9']
    ]
    tips20_no_a = [
        ctx.load_labware('opentrons_96_filtertiprack_20ul', '11',
        '20µl tiprack - no tips in row A')
    ]
    tips300 = [ctx.load_labware('opentrons_96_filtertiprack_200ul', '10')]
    tempdeck = ctx.load_module('Temperature Module Gen2', '4')
    pcr_plate = tempdeck.load_labware(
        'opentrons_96_aluminumblock_biorad_wellplate_200ul', 'PCR plate')
    mm_strips = ctx.load_labware(
        'opentrons_96_aluminumblock_generic_pcr_strip_200ul', '5',
        'mastermix strips')
    tempdeck.set_temperature(4)
    tube_block = ctx.load_labware(
        'opentrons_24_aluminumblock_nest_1.5ml_snapcap', '2',
        '2ml screw tube aluminum block for mastermix + controls')

    # pipette
    m20 = ctx.load_instrument('p20_multi_gen2', 'right', tip_racks=tips20)
    p300 = ctx.load_instrument('p300_single_gen2', 'left', tip_racks=tips300)

    # setup up sample sources and destinations
    num_cols = math.ceil(NUM_SAMPLES/8)
    sources = source_plate.rows()[0][:num_cols]
    sample_dests = pcr_plate.rows()[0][:num_cols]

    tip_log = {'count': {}}
    folder_path = '/data/C'
    tip_file_path = folder_path + '/tip_log.json'
    if TIP_TRACK and not ctx.is_simulating():
        if os.path.isfile(tip_file_path):
            with open(tip_file_path) as json_file:
                data = json.load(json_file)
                if 'tips20' in data:
                    tip_log['count'][m20] = data['tips20']
                else:
                    tip_log['count'][m20] = 0
                if 'tips300' in data:
                    tip_log['count'][p300] = data['tips300']
                else:
                    tip_log['count'][p300] = 0
                if 'tips20_no_a' in data:
                    tip_log['count']['tips20_no_a'] = data['tips20_no_a']
                else:
                    tip_log['count']['tips20_no_a'] = 0
        else:
            tip_log['count'] = {m20: 0, p300: 0, 'tips20_no_a': 0}
    else:
        tip_log['count'] = {m20: 0, p300: 0, 'tips20_no_a': 0}

    tip_log['tips'] = {
        m20: [tip for rack in tips20 for tip in rack.rows()[0]],
        p300: [tip for rack in tips300 for tip in rack.wells()],
        'tips20_no_a': [tip for rack in tips20_no_a for tip in rack.rows()[0]]
    }
    tip_log['max'] = {
        pip: len(tip_log['tips'][pip])
        for pip in [m20, p300, 'tips20_no_a']
    }

    def pick_up(pip):
        nonlocal tip_log
        if tip_log['count'][pip] == tip_log['max'][pip]:
            ctx.pause('Replace ' + str(pip.max_volume) + 'µl tipracks before \
resuming.')
            pip.reset_tipracks()
            tip_log['count'][pip] = 0
        pip.pick_up_tip(tip_log['tips'][pip][tip_log['count'][pip]])
        tip_log['count'][pip] += 1

    def pick_up_no_a():
        nonlocal tip_log
        if tip_log['count']['tips20_no_a'] == tip_log['max']['tips20_no_a']:
            ctx.pause('Replace 20ul tiprack in slot 10 (without tips in row A) \
before resuming.')
            tip_log['count']['tips20_no_a'] = 0
        m20.pick_up_tip(tip_log['tips']['tips20_no_a'][tip_log['count']['tips20_no_a']])
        tip_log['count']['tips20_no_a'] += 1

    """ mastermix component maps """
    mm_tube = tube_block.wells()[0]
    mm_dict = {
        'volume': 12,
        'components': {
            tube: vol
            for tube, vol in zip(tube_block.columns()[1], [10, 2])
        }
    }

    if PREPARE_MASTERMIX:
        vol_overage = 1.2  # decrease overage for small sample number

        for i, (tube, vol) in enumerate(mm_dict['components'].items()):
            comp_vol = vol*(NUM_SAMPLES)*vol_overage
            pick_up(p300)
            num_trans = math.ceil(comp_vol/160)
            vol_per_trans = comp_vol/num_trans
            for _ in range(num_trans):
                p300.air_gap(20)
                p300.aspirate(vol_per_trans, tube)
                ctx.delay(seconds=3)
                p300.touch_tip(tube)
                p300.air_gap(20)
                p300.dispense(20, mm_tube.top())  # void air gap
                p300.dispense(vol_per_trans, mm_tube.bottom(2))
                p300.dispense(20, mm_tube.top())  # void pre-loaded air gap
                p300.blow_out(mm_tube.top())
                p300.touch_tip(mm_tube)
            if i < len(mm_dict['components'].items()) - 1:  # only keep tip if last component and p300 in use
                p300.drop_tip()
        mm_total_vol = mm_dict['volume']*(NUM_SAMPLES)*vol_overage
        if not p300.hw_pipette['has_tip']:  # pickup tip with P300 if necessary for mixing
            pick_up(p300)
        mix_vol = mm_total_vol / 2 if mm_total_vol / 2 <= 200 else 200  # mix volume is 1/2 MM total, maxing at 200µl
        mix_loc = mm_tube.bottom(20) if NUM_SAMPLES > 48 else mm_tube.bottom(5)
        p300.mix(7, mix_vol, mix_loc)
        p300.blow_out(mm_tube.top())
        p300.touch_tip()

    # transfer mastermix to strips
    vol_per_strip_well = num_cols*mm_dict['volume']*1.1
    mm_strip = mm_strips.columns()[0]
    if not p300.hw_pipette['has_tip']:
        pick_up(p300)
    for well in mm_strip:
        p300.transfer(vol_per_strip_well, mm_tube, well, new_tip='never')

    # transfer mastermix to plate
    mm_vol = mm_dict['volume']
    pick_up(m20)
    m20.transfer(mm_vol, mm_strip[0].bottom(0.5), sample_dests,
                 new_tip='never')
    m20.drop_tip()

    # transfer samples to corresponding locations
    sample_vol = 20 - mm_vol
    for i, (s, d) in enumerate(zip(sources, sample_dests)):
        if i == 9:
            pick_up_no_a()
        else:
            pick_up(m20)
        m20.transfer(sample_vol, s.bottom(2), d.bottom(2), new_tip='never')
        m20.mix(1, 10, d.bottom(2))
        m20.blow_out(d.top(-2))
        m20.aspirate(5, d.top(2))  # suck in any remaining droplets on way to trash
        m20.drop_tip()

    # track final used tip
    if TIP_TRACK and not ctx.is_simulating():
        if not os.path.isdir(folder_path):
            os.mkdir(folder_path)
        data = {
            'tips20': tip_log['count'][m20],
            'tips300': tip_log['count'][p300],
            'tips20_no_a': tip_log['count']['tips20_no_a']
        }
        with open(tip_file_path, 'w') as outfile:
            json.dump(data, outfile)
