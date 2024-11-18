# import sys

# from atproto import CAR, FirehoseSubscribeReposClient, parse_subscribe_repos_message, models, parse_subscribe_labels_message, firehose_models


# class BlueHoover:
#     def __init__(self):
#         self.client = FirehoseSubscribeReposClient()

#     def start(self):
#         self.client.start(self.on_message_handler)


#     def on_message_handler(self, message: firehose_models.MessageFrame) -> None:
#         labels_message = parse_subscribe_labels_message(message)
#         if not isinstance(labels_message, models.ComAtprotoLabelSubscribeLabels.Labels):
#             return

#         for label in labels_message.labels:
#             neg = '(NEG)' if label.neg else ''
#             print(f'[{label.cts}] ({label.src}) {label.uri} => {label.val} {neg}')

#     # def on_message_handler(self, message) -> None:
#     #     commit = parse_subscribe_repos_message(message)
#     #     # we need to be sure that it's commit message with .blocks inside
#     #     if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
#     #         return

#     #     if not commit.blocks:
#     #         return

#     #     car = CAR.from_bytes(commit.blocks)

#     #     for record in car:
#     #         print(record)

#     #     print(car)
#     #     sys.exit()


if __name__ == "__main__":
    # bluehoover = BlueHoover()

    # bluehoover.start()

    from atproto import FirehoseSubscribeLabelsClient, firehose_models, models, parse_subscribe_labels_message

    client = FirehoseSubscribeLabelsClient()


    def on_message_handler(message: firehose_models.MessageFrame) -> None:
        labels_message = parse_subscribe_labels_message(message)
        if not isinstance(labels_message, models.ComAtprotoLabelSubscribeLabels.Labels):
            return

        for label in labels_message.labels:
            neg = '(NEG)' if label.neg else ''
            print(f'[{label.cts}] ({label.src}) {label.uri} => {label.val} {neg}')


    client.start(on_message_handler)