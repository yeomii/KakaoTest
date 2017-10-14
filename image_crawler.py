import sys, time
import http.client, urllib.parse
import json

import settings


DOC_CATEGORIES = []
DOC_NEXT_URLS = {}
DOC_PAGES = {}
IMAGES = {}
FEATURE_QUEUE = []
UPDATE_QUEUE = {}
POST_QUEUE = []
DEL_QUEUE = []

API_CALL_COUNT = 0


def create_token():
    global API_CALL_COUNT
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("GET", settings.TOKEN_API + settings.LOGIN_TOKEN)
        API_CALL_COUNT += 1
        response = conn.getresponse()
        print(response.status, response.reason)
        if response.status == 200 or response.status == 403:
            data = response.read().decode("UTF-8")
            print("submit token : ", data)
            settings.SUBMIT_TOKEN = data
            conn.close()
            return True
        else:
            print(response.status, response.reason)
            conn.close()
            return False
    except Exception as e:
        print("Error:", e)
        return False


def get_document_seed():
    global API_CALL_COUNT
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("GET", settings.SEED_API, headers={'X-Auth-Token': settings.SUBMIT_TOKEN})
        API_CALL_COUNT += 1
        response = conn.getresponse()
        if response.status == 200:
            data = response.read().decode("UTF-8")
            print(data)
            conn.close()
            for l in data.split('\n'):
                if len(l) > len(settings.DOCUMENT_API):
                    category = l.split('/')[2]
                    DOC_NEXT_URLS[category] = l
                    DOC_CATEGORIES.append(category)
                    DOC_PAGES[category] = 0
            return True
        else:
            print(response.status, response.reason)
            conn.close()
            return False
    except Exception as e:
        print("Error:", e)
        return False


def get_images(category):
    global API_CALL_COUNT
    print("get_images called...")
    url = DOC_NEXT_URLS[category]
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("GET", url, headers={'X-Auth-Token': settings.SUBMIT_TOKEN})
        API_CALL_COUNT += 1
        response = conn.getresponse()
        if response.status == 200:
            data = json.loads(response.read().decode("UTF-8"))
            #print(data)
            conn.close()
            if url == data['next_url']:
                print("same page!")
                #time.sleep(1)
            else:
                print("%s category %d th page - %d images" % (category, DOC_PAGES[category], len(data['images'])))
                DOC_NEXT_URLS[category] = data['next_url']
                DOC_PAGES[category] += 1
                update_images(category, data['images'])
            return True
        else:
            print(response.status, response.reason)
            conn.close()
            if response.status == 401:
                exit()
            return False
    except Exception as e:
        print("Error:", e)
        return False


def update_images(category, images):
    for image in images:
        if image['id'] not in IMAGES:
            IMAGES[image['id']] = {'feature': None, 'operation': []}
            FEATURE_QUEUE.append(image['id'])
            IMAGES[image['id']]['last_op'] = -1
        IMAGES[image['id']]['operation'].append(tuple([image['type'], category, DOC_PAGES[category]]))
        UPDATE_QUEUE[image['id']] = image['id']
        #print("image", image['id'], IMAGES[image['id']])


def extract_image_feature():
    global API_CALL_COUNT
    global FEATURE_QUEUE
    print("extract image feature - %d" % len(FEATURE_QUEUE))

    count = min(50, len(FEATURE_QUEUE))
    if count == 0:
        return
    params = urllib.parse.urlencode({
        'id': ','.join(FEATURE_QUEUE[:count])
    })
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("GET", settings.IMAGE_FEATURE_API + '?' + params, headers={'X-Auth-Token': settings.SUBMIT_TOKEN})
        API_CALL_COUNT += 1
        response = conn.getresponse()
        if response.status == 200:
            data = json.loads(response.read().decode("UTF-8"))
            #print(data)
            conn.close()
            queue_copy = FEATURE_QUEUE[:count]
            for image_feature in data['features']:
                IMAGES[image_feature['id']]['feature'] = image_feature['feature']
                queue_copy.remove(image_feature['id'])
            FEATURE_QUEUE = queue_copy + FEATURE_QUEUE[count:]
        else:
            print(response.status, response.reason)
            conn.close()
            return False
    except Exception as e:
        print("Error:", e)
        return False


def calc_operation():
    print("calc_operation : UPDATE QUEUE length - %d" % len(UPDATE_QUEUE))
    update_ids = UPDATE_QUEUE.keys()
    for image_id in update_ids:
        image = IMAGES[image_id]
        if len(image['operation']) == image['last_op']:
            continue
        if image['last_op'] == -1:
            if image['operation'][-1][0] == 'del':
                image['last_op'] = len(image['operation']) - 1
            else:
                POST_QUEUE.append(image_id)
        else:
            if image['operation'][image['last_op']][0] == image['operation'][-1][0]:
                image['last_op'] = len(image['operation']) - 1
            else:
                if image['operation'][-1][0] == 'del':
                    try:
                        POST_QUEUE.remove(image_id)
                    except ValueError:
                        pass
                    DEL_QUEUE.append(image_id)
                else:
                    try:
                        DEL_QUEUE.remove(image_id)
                    except ValueError:
                        pass
                    POST_QUEUE.append(image_id)
    UPDATE_QUEUE.clear()


def post_images():
    global API_CALL_COUNT
    print("post images - %d" % len(POST_QUEUE))
    post_target = {'data': []}
    unknown_feature_images = 0
    for image_id in POST_QUEUE:
        if IMAGES[image_id]['feature'] is None:
            unknown_feature_images += 1
        else:
            post_target['data'].append(dict({'id': image_id, 'feature': IMAGES[image_id]['feature']}))
        if len(post_target['data']) >= 50:
            break

    if len(post_target['data']) == 0:
        return
    print("Error!!!! in post_images : unknown_feature_images - %d" % unknown_feature_images)
    data = json.dumps(post_target)
    #print(data)
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("POST", settings.IMAGE_FEATURE_API, body=data, headers={'X-Auth-Token': settings.SUBMIT_TOKEN})
        API_CALL_COUNT += 1
        response = conn.getresponse()
        conn.close()
        if response.status == 200:
            for image in post_target['data']:
                POST_QUEUE.remove(image['id'])
                IMAGES[image['id']]['last_op'] = len(IMAGES[image['id']]['operation']) - 1
        else:
            print(response.status, response.reason)
            conn.close()
            return False
    except Exception as e:
        print("Error:", e)
        return False


def del_images():
    global API_CALL_COUNT
    print("delete images - %d" % len(POST_QUEUE))
    del_target = {'data': []}
    for image_id in DEL_QUEUE:
        del_target['data'].append(dict({'id': image_id}))
        if len(del_target['data']) >= 50:
            break

    if len(del_target['data']) == 0:
        return

    data = json.dumps(del_target)
    #print(data)
    try:
        conn = http.client.HTTPConnection(settings.URL_BASE)
        conn.request("DELETE", settings.IMAGE_FEATURE_API, body=data, headers={'X-Auth-Token': settings.SUBMIT_TOKEN})
        API_CALL_COUNT += 1
        response = conn.getresponse()
        conn.close()
        if response.status == 200:
            for image in del_target['data']:
                DEL_QUEUE.remove(image['id'])
                IMAGES[image['id']]['last_op'] = len(IMAGES[image['id']]['operation']) - 1
        else:
            print(response.status, response.reason)
            conn.close()
            return False
    except Exception as e:
        print("Error:", e)
        return False

if __name__ == "__main__":

    print("usage : image_crawler.py <category> <log_file>")
    category = None
    log_file = None
    fp = None
    if len(sys.argv) > 1:
        category = sys.argv[1]
    if len(sys.argv) > 2:
        log_file = sys.argv[2]
        fp = open(log_file, "w")
        sys.stdout = fp

    while settings.SUBMIT_TOKEN is None:
        create_token()
    while len(DOC_CATEGORIES) == 0:
        get_document_seed()
    print(DOC_NEXT_URLS)
    print(DOC_CATEGORIES)

    if category is not None and DOC_CATEGORIES.index(category) >= 0:
        DOC_CATEGORIES = [category]
        print(DOC_CATEGORIES)

    while True:
        for c in DOC_CATEGORIES:
            get_images(c)

        while len(FEATURE_QUEUE) >= 50:
            prev_features = len(FEATURE_QUEUE)
            extract_image_feature()
            if len(FEATURE_QUEUE) == prev_features:
                break

        while len(UPDATE_QUEUE) > 0:
            calc_operation()

        while len(POST_QUEUE) >= 50:
            prev_post_images = len(POST_QUEUE)
            post_images()
            if len(POST_QUEUE) == prev_post_images:
                break

        while len(DEL_QUEUE) >= 50:
            prev_del_images = len(DEL_QUEUE)
            del_images()
            if len(DEL_QUEUE) == prev_del_images:
                break
        print("api call count : ", API_CALL_COUNT)

    if fp is not None:
        fp.close()
